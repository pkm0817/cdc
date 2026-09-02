package dev.embeddedcdc.application;

import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.FailureVerdict;
import dev.embeddedcdc.domain.model.SourceTable;
import dev.embeddedcdc.domain.port.in.ChangeEventHandler;
import dev.embeddedcdc.domain.port.out.DeadLetterStore;
import dev.embeddedcdc.domain.port.out.FailureClassifier;
import dev.embeddedcdc.domain.port.out.PipelineMetrics;
import dev.embeddedcdc.infrastructure.config.CdcApplyProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 배치 하나를 끝까지 책임진다 — 재시도, 격리, 정지 판단.
 *
 * 이전 구현은 예외를 삼키고 다음 이벤트로 넘어갔다. 파이프라인은 멈추지 않았지만
 * 그 한 건은 재시도 없이 사라졌다. 지금은 실패를 세 갈래로 나눈다.
 *
 *   RETRY       배치 전체를 백오프 후 다시 시도한다
 *   DEAD_LETTER 건 단위로 좁혀 범인만 격리하고 나머지는 반영한다
 *   HALT        예외를 밖으로 던진다. 오프셋이 전진하지 않아 유실이 없다
 *
 * 격리 비율이 임계를 넘으면 데이터 문제가 아니라 구조 문제로 보고 멈춘다 —
 * 그러지 않으면 수신 테이블이 통째로 사라졌을 때 DLQ 가 전체 트래픽을 삼킨다.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ChangeEventService implements ChangeEventHandler {

    private final BatchApplier applier;
    private final FailureClassifier classifier;
    private final DeadLetterStore deadLetters;
    private final PipelineMetrics metrics;
    private final CdcApplyProperties props;

    @Override
    public void handle(String pipeline, List<ChangeEvent> batch) {
        if (batch.isEmpty()) {
            return;
        }

        try {
            applier.applyAll(pipeline, batch);
            recordSuccess(batch);
            return;
        } catch (Exception first) {
            haltIfUnrecoverable(first);
            log.warn("배치 적용 실패 ({}건) — 재시도로 넘어간다: {}", batch.size(), first.toString());
        }

        if (retryBatch(pipeline, batch)) {
            return;
        }

        // 여기까지 왔으면 배치 전체 재시도로는 풀리지 않는다. 범인을 찾아야 한다.
        isolate(pipeline, batch);
    }

    /** 배치 전체를 백오프하며 다시 시도한다. 성공하면 true. */
    private boolean retryBatch(String pipeline, List<ChangeEvent> batch) {
        for (int attempt = 1; attempt <= props.maxBatchRetries(); attempt++) {
            backoff(attempt);
            try {
                applier.applyAll(pipeline, batch);
                recordSuccess(batch);
                log.info("배치 재시도 성공 (시도 {}회, {}건)", attempt, batch.size());
                return true;
            } catch (Exception e) {
                haltIfUnrecoverable(e);
                log.warn("배치 재시도 {}/{} 실패: {}", attempt, props.maxBatchRetries(), e.toString());
            }
        }
        return false;
    }

    /**
     * 건 단위로 적용하며 실패한 것만 격리한다.
     * 여기까지 온 이벤트는 배치 재시도를 이미 소진했으므로, 일시 장애도 격리 대상이 된다.
     */
    private void isolate(String pipeline, List<ChangeEvent> batch) {
        List<Failure> failures = new ArrayList<>();
        long highestLsn = 0;

        for (ChangeEvent event : batch) {
            try {
                applier.applyOne(pipeline, event);
                recordApplied(event);
            } catch (Exception e) {
                haltIfUnrecoverable(e);
                failures.add(new Failure(event, e));
            }
            highestLsn = Math.max(highestLsn, event.lsn());
        }

        // 정지 판단이 DLQ 기록보다 먼저다.
        // 멈출 상황이면 애초에 개별 데이터 문제가 아니므로 격리해서는 안 된다 —
        // 기록해 두면 재기동 후 정상 적용된 뒤에도 DLQ 에 남아 회계가 어긋난다.
        double ratio = (double) failures.size() / batch.size();
        if (ratio > props.haltOnDeadLetterRatio()) {
            metrics.pipelineHalted("DLQ_RATIO");
            throw new PipelineHaltedException(String.format(
                    "격리 비율이 임계를 넘었다 (%d/%d = %.0f%%, 임계 %.0f%%). "
                            + "개별 데이터 문제가 아니라 구조 문제로 본다. 오프셋을 전진시키지 않고 멈춘다",
                    failures.size(), batch.size(), ratio * 100, props.haltOnDeadLetterRatio() * 100));
        }

        for (Failure failure : failures) {
            deadLetters.store(pipeline, failure.event(), failure.cause(), props.maxBatchRetries() + 1);
            metrics.deadLettered(failure.event().table());
            log.error("격리 table={} op={} lsn={} 사유={}",
                    failure.event().table(), failure.event().op().code(),
                    failure.event().lsn(), failure.cause().toString());
        }

        // 격리된 건도 DLQ 에 남아 추적되므로, 여기까지 왔으면 진행 지점을 올려도 안전하다.
        if (highestLsn > 0) {
            applier.recordCheckpoint(pipeline, highestLsn);
        }
        if (!failures.isEmpty()) {
            log.warn("배치 {}건 중 {}건 격리 — DLQ 에서 원인 확인 후 재처리할 것",
                    batch.size(), failures.size());
        }
    }

    /** 격리 후보. 정지 판단이 끝날 때까지 기록하지 않고 들고 있는다. */
    private record Failure(ChangeEvent event, Exception cause) {
    }

    private void haltIfUnrecoverable(Exception e) {
        if (e instanceof PipelineHaltedException halted) {
            throw halted;   // 재던지기 — 여기서는 세지 않는다 (중복 계수)
        }
        if (classifier.classify(e) == FailureVerdict.HALT) {
            metrics.pipelineHalted("UNRECOVERABLE");
            throw new PipelineHaltedException(
                    "계속 돌리면 안 되는 실패다. 오프셋을 전진시키지 않고 멈춘다: " + e, e);
        }
    }

    private void recordSuccess(List<ChangeEvent> batch) {
        for (ChangeEvent event : batch) {
            recordApplied(event);
        }
    }

    /**
     * 반영된 이벤트만 센다.
     *
     * 배치에는 우리가 다루지 않는 테이블의 이벤트도 섞여 온다 — heartbeat 테이블이 그렇다.
     * 그것까지 세면 유휴 구간에도 cdc_events_total 이 꾸준히 오르고, 처리량 패널이
     * "일이 있다"고 말한다. 진행 지점(체크포인트)은 그 이벤트로도 전진해야 하지만
     * (그래야 슬롯과 어긋나지 않는다) 처리량은 아니다. 둘은 다른 질문이다.
     */
    private void recordApplied(ChangeEvent event) {
        if (SourceTable.fromName(event.table()).isEmpty()) {
            return;
        }
        metrics.eventApplied(event.table(), event.op().code());
        metrics.endToEndLag(event.table(), event.sourceTsMs());
    }

    private void backoff(int attempt) {
        long millis = props.retryBackoffMs() * (long) Math.pow(2, attempt - 1);
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PipelineHaltedException("재시도 대기 중 인터럽트 — 종료 중으로 본다", e);
        }
    }
}
