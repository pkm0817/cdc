package dev.cdccustom.infrastructure.health;

import dev.cdccustom.application.SyncRunner;
import dev.cdccustom.application.SyncWorker;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * 루프의 상태를 판정한다.
 *
 * <p>두 가지를 본다.
 * <ul>
 *   <li><b>연속 실패</b> — 3회 이상이면 DOWN. 처음 이 클래스는 "마지막으로 한 주기를 돈
 *       시각"만 봤는데, 그 값은 실패해도 갱신되므로 매 주기 실패하는 상태에서도 UP 이었다.
 *       실제로 그 상태를 만나 보고서야 드러난 구멍이라 여기 적어 둔다.</li>
 *   <li><b>정체</b> — 60초 넘게 한 주기도 못 돌았으면 DOWN. 루프가 죽었거나 한 배치에
 *       갇혀 있다는 뜻이다.</li>
 * </ul>
 *
 * <p>밀린 양(pending)으로는 판정하지 않는다. 대량 적재 직후에는 정상적으로도 크게 밀리며,
 * 그것은 건강 문제가 아니라 처리 중인 상태다.
 */
@Component
public class SyncHealthIndicator implements HealthIndicator {

    private static final Duration STALL_THRESHOLD = Duration.ofSeconds(60);
    private static final int FAILURE_THRESHOLD = 3;

    private final SyncWorker worker;
    private final SyncRunner runner;

    public SyncHealthIndicator(SyncWorker worker, SyncRunner runner) {
        this.worker = worker;
        this.runner = runner;
    }

    @Override
    public Health health() {
        Duration since = Duration.between(worker.lastRunAt(), Instant.now());
        int failures = runner.consecutiveFailures();

        boolean down = failures >= FAILURE_THRESHOLD || since.compareTo(STALL_THRESHOLD) > 0;
        Health.Builder builder = down ? Health.down() : Health.up();

        builder.withDetail("lastAppliedSeq", worker.lastAppliedSeq())
                .withDetail("secondsSinceLastRun", since.toSeconds())
                .withDetail("consecutiveFailures", failures);
        if (runner.lastFailure() != null) {
            builder.withDetail("lastFailure", runner.lastFailure());
        }
        return builder.build();
    }
}
