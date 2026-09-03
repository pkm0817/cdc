package dev.cdccustom.infrastructure.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 지표.
 *
 * <p>이름과 라벨을 CDC 두 스택과 <b>일부러 맞췄다</b>. {@code cdc_events_total},
 * {@code cdc_end_to_end_lag_seconds} 는 같은 뜻으로 쓰이므로 세 스택을 한 대시보드에서
 * 같은 쿼리로 비교할 수 있다. (Micrometer 는 점을 밑줄로 바꿔 내보낸다 —
 * {@code cdc.end.to.end.lag} 가 {@code cdc_end_to_end_lag_seconds} 가 된다)
 *
 * <p>여기에만 있는 지표가 셋 있다.
 * <ul>
 *   <li>{@code sync_outbox_entries_total} — 접기 <b>전</b> 줄 수</li>
 *   <li>{@code sync_folded_rows_total} — 접은 <b>뒤</b> 반영한 행 수</li>
 *   <li>{@code sync_outbox_pending} — 아직 반영되지 않은 줄 수 (CDC 의 슬롯 지연에 해당)</li>
 * </ul>
 * 앞의 둘을 나누면 접기 비율이 나온다. 이 값이 1 에 가까우면 이 방식을 쓸 이유가 없다는
 * 뜻이므로 판단 근거로 계속 노출한다.
 */
@Component
public class SyncMetrics {

    private final MeterRegistry registry;
    private final Map<String, Counter> appliedCounters = new ConcurrentHashMap<>();
    private final Map<String, Timer> lagTimers = new ConcurrentHashMap<>();
    private final Counter rawEntries;
    private final Counter foldedRows;
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong clockSkewMs = new AtomicLong();

    public SyncMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.rawEntries = Counter.builder("sync.outbox.entries")
                .description("outbox 에서 읽은 줄 수 (접기 전)")
                .register(registry);
        this.foldedRows = Counter.builder("sync.folded.rows")
                .description("접은 뒤 실제로 반영한 행 수")
                .register(registry);
        registry.gauge("sync.outbox.pending", pending, AtomicLong::doubleValue);
        // 이름을 CDC 두 스택과 맞춘다 — cdc_clock_skew_seconds. 지연 경보의 게이트가 보는 값이다.
        Gauge.builder("cdc.clock.skew.seconds", clockSkewMs, v -> v.get() / 1000.0)
                .description("source DB 시계와 이 프로세스 시계의 차 (왕복 보정)")
                .register(registry);
    }

    /** 표·연산별 반영 건수. CDC 판과 이름·라벨이 같다. */
    public void applied(String table, String op, int count) {
        appliedCounters.computeIfAbsent(table + "|" + op, key ->
                        Counter.builder("cdc.events")
                                .description("타깃에 반영한 행 수")
                                .tag("table", table)
                                .tag("op", op)
                                .register(registry))
                .increment(count);
    }

    /**
     * 변경이 소스에 기록된 시각과 타깃 반영 시각의 차.
     *
     * <p>Timer 의 max 는 시간이 지나면 스스로 내려간다. 부하가 멎었는데 한 번 튄 최댓값이
     * 대시보드에 계속 남는 문제를 피하려면 게이지가 아니라 Timer 여야 한다
     * (Go 판에서 실제로 그 문제를 겪고 고쳤다).
     */
    public void lag(String table, Duration lag) {
        lagTimers.computeIfAbsent(table, t ->
                        Timer.builder("cdc.end.to.end.lag")
                                .description("소스 변경 기록 시각과 타깃 반영 시각의 차")
                                .tag("table", t)
                                .register(registry))
                .record(lag);
    }

    public void batchApplied(int rawCount, int foldedCount) {
        rawEntries.increment(rawCount);
        foldedRows.increment(foldedCount);
    }

    public void pending(long count) {
        pending.set(count);
    }

    /**
     * source DB 와의 시계 편차. {@link SourceClockSkewProbe} 가 채운다.
     *
     * <p>못 잰 경우에는 호출되지 않는다 — 마지막으로 잰 값이 그대로 남는다. 0 으로 덮으면
     * "편차 없음" 으로 읽혀 지연 경보의 게이트가 열린 채로 남기 때문이다.
     */
    public void clockSkew(long skewMs) {
        clockSkewMs.set(skewMs);
    }
}
