package dev.embeddedcdc.infrastructure.metrics;

import dev.embeddedcdc.domain.port.out.PipelineMetrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PipelineMetrics 의 Micrometer 구현. Prometheus 로 노출된다.
 *
 * 미터 이름을 바꾸면 Grafana 대시보드가 조용히 빈 패널이 된다. 바꾸려면 대시보드 JSON 도 같이 고칠 것.
 */
@Component
@RequiredArgsConstructor
public class MicrometerPipelineMetrics implements PipelineMetrics {

    private final MeterRegistry registry;

    private final AtomicInteger captureGap = new AtomicInteger(0);

    /**
     * source DB 시계 - 이 프로세스 시계 (밀리초).
     *
     * 프로브가 재는 데 실패하면 여기를 건드리지 않는다 — 마지막으로 잰 값이 그대로 남는다.
     * 실패를 0(정상)으로 덮으면 "편차 없음"과 "못 쟀음"이 같은 그림이 되기 때문이다.
     * 못 재고 있다는 사실은 프로브의 경고 로그와 스크랩이 멈춘 시계열로 드러난다.
     */
    private final AtomicLong clockSkewMs = new AtomicLong(0);

    /**
     * Gauge 는 등록 시점에 한 번 묶어 둔다. 값이 바뀔 때 등록하는 게 아니라
     * 참조를 걸어두고 registry 가 스크랩마다 읽어가는 구조다.
     *
     * 기동 직후부터 0 이 노출돼야 한다 — 갭이 없을 때 시계열이 아예 없으면
     * 경보식(cdc_capture_gap == 1)이 "없음"과 "정상"을 구분하지 못한다.
     */
    @PostConstruct
    void bindGauges() {
        Gauge.builder("cdc.capture.gap", captureGap, AtomicInteger::get)
             .description("1 if a capture gap was detected at startup")
             .register(registry);

        // 지연 지표의 신뢰도 판정에 쓰인다. 초 단위로 내보내 지연(초)과 같은 축에서 견줄 수 있게 한다.
        Gauge.builder("cdc.clock.skew.seconds", clockSkewMs, v -> v.get() / 1000.0)
             .description("source DB clock minus this process clock, in seconds")
             .register(registry);
    }

    @Override
    public void eventApplied(String table, String op) {
        registry.counter("cdc.events", "table", table, "op", op).increment();
    }

    @Override
    public void applyFailed(String table) {
        registry.counter("cdc.sink.errors", "table", table).increment();
    }

    @Override
    public void deadLettered(String table) {
        registry.counter("cdc.dead.letters", "table", table).increment();
    }

    @Override
    public void endToEndLag(String table, long sourceCommittedAtMs) {
        if (sourceCommittedAtMs <= 0) {
            return; // 스냅샷 이벤트 등 커밋 시각이 없는 경우
        }
        long lagMs = Math.max(0, System.currentTimeMillis() - sourceCommittedAtMs);
        registry.timer("cdc.end.to.end.lag", "table", table).record(Duration.ofMillis(lagMs));
    }

    @Override
    public void captureGap(boolean detected) {
        captureGap.set(detected ? 1 : 0);
    }

    @Override
    public void pipelineHalted(String reason) {
        registry.counter("cdc.pipeline.halts", "reason", reason).increment();
    }

    @Override
    public void clockSkew(long skewMs) {
        clockSkewMs.set(skewMs);
    }

    @Override
    public void changeAudited(String table) {
        registry.counter("cdc.change.audit.rows", "table", table).increment();
    }
}
