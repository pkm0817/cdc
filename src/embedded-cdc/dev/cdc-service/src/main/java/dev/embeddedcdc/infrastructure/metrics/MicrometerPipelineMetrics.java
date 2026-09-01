package dev.embeddedcdc.infrastructure.metrics;

import dev.embeddedcdc.domain.port.out.PipelineMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * PipelineMetrics 의 Micrometer 구현. Prometheus 로 노출된다.
 *
 * 미터 이름을 바꾸면 Grafana 대시보드가 조용히 빈 패널이 된다. 바꾸려면 대시보드 JSON 도 같이 고칠 것.
 */
@Component
@RequiredArgsConstructor
public class MicrometerPipelineMetrics implements PipelineMetrics {

    private final MeterRegistry registry;

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
}
