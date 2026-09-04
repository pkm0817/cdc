package dev.embeddedcdc.application;

import dev.embeddedcdc.domain.model.PendingDeadLetter;
import dev.embeddedcdc.domain.port.out.DeadLetterStore;
import dev.embeddedcdc.infrastructure.config.CdcSourceProperties;
import dev.embeddedcdc.infrastructure.config.DeadLetterProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 격리된 이벤트를 다시 반영한다.
 *
 * <b>PENDING 을 자동으로 집지 않는다.</b> 원인이 고쳐졌는지는 사람만 안다 —
 * 자동 재시도를 돌리면 고쳐지지 않은 독성 건이 영원히 재시도되며 잡음만 쌓인다.
 * 운영자가 원인을 고친 뒤 상태를 바꾸는 것이 곧 재처리 신청이다.
 *
 * <pre>
 * UPDATE cdc_dead_letter SET status = 'RETRY_REQUESTED' WHERE id = 1;
 * </pre>
 *
 * <b>재처리가 안전한 이유는 LSN 가드에 있다.</b> 격리된 뒤 같은 행에 더 새로운 변경이
 * 이미 반영됐다면, 오래된 LSN 을 든 이 이벤트는 갱신 행 수 0 으로 차단된다.
 * 즉 순서가 뒤바뀐 재처리가 최신 값을 덮어쓰지 않는다.
 *
 * 다만 그 가드는 computer 에만 있다 — car 는 조건 없는 UPSERT 라
 * 재처리가 더 새로운 값을 덮어쓸 수 있다. car 를 재처리할 때는 그 사이 변경이 없었는지
 * 확인해야 한다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(prefix = "cdc.dead-letter", name = "reprocess-enabled",
        havingValue = "true", matchIfMissing = true)
public class DeadLetterReprocessor {

    /** 재처리가 실제로 행을 바꿨다. */
    static final String RESOLUTION_APPLIED = "APPLIED";
    /** 더 새로운 값이 이미 있어 LSN 가드에 막혔다. 복구는 끝난 것으로 본다. */
    static final String RESOLUTION_STALE_SKIPPED = "STALE_SKIPPED";

    private final DeadLetterStore deadLetters;
    private final BatchApplier applier;
    private final CdcSourceProperties source;
    private final DeadLetterProperties props;

    @Scheduled(fixedDelayString = "${cdc.dead-letter.reprocess-interval-ms:30000}")
    public void reprocess() {
        List<PendingDeadLetter> claimed =
                deadLetters.claimForRetry(source.name(), props.reprocessBatchSize());
        if (claimed.isEmpty()) {
            return;
        }

        log.info("DLQ 재처리 시작 — {}건", claimed.size());
        int applied = 0;
        int skipped = 0;
        int failed = 0;

        for (PendingDeadLetter pending : claimed) {
            try {
                // 체크포인트를 올리지 않는 경로다 — 지나간 LSN 을 다시 적용하는 것이므로
                // 진행 지점을 건드리면 안 된다.
                int affected = applier.applyOne(source.name(), pending.event());

                // 예외가 없다고 다 반영된 것이 아니다. 격리된 뒤 재처리까지의 사이에
                // 정상 경로로 더 새로운 값이 들어왔으면 LSN 가드가 0행으로 막는다.
                // 그것도 복구 완료이긴 하지만 "반영했다"와는 다른 사실이라 갈라서 남긴다 —
                // 둘을 뭉뚱그리면 라우팅 오류나 대상 부재까지 성공으로 보인다.
                if (affected > 0) {
                    deadLetters.markResolved(pending.id(), RESOLUTION_APPLIED);
                    applied++;
                    log.info("재처리 반영 dlqId={} table={} op={} lsn={} 행={}",
                            pending.id(), pending.event().table(),
                            pending.event().op().code(), pending.event().lsn(), affected);
                } else {
                    deadLetters.markResolved(pending.id(), RESOLUTION_STALE_SKIPPED);
                    skipped++;
                    log.info("재처리 차단 dlqId={} table={} op={} lsn={} — 더 새로운 값이 이미 있다",
                            pending.id(), pending.event().table(),
                            pending.event().op().code(), pending.event().lsn());
                }
            } catch (Exception e) {
                deadLetters.markRetryFailed(pending.id(), e);
                failed++;
                log.warn("재처리 실패 dlqId={} table={} lsn={} 사유={} — PENDING 으로 되돌림",
                        pending.id(), pending.event().table(), pending.event().lsn(), e.toString());
            }
        }

        log.info("DLQ 재처리 완료 — 반영 {}건, 차단 {}건, 실패 {}건", applied, skipped, failed);
    }
}
