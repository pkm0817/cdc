package dev.cdccustom.infrastructure.metrics;

import dev.cdccustom.infrastructure.config.SyncProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * source DB 와 이 프로세스의 시계 차이를 주기적으로 잰다.
 *
 * <p><b>왜 이 방식에도 필요한가.</b> 여기서 지연은 폴링 주기가 지배하니 편차는 묻힐 것 같지만,
 * {@code cdc_end_to_end_lag_seconds} 가 재는 것은 여전히 <b>outbox 에 기록된 시각(DB 시계)</b>과
 * <b>반영 시각(앱 시계)</b>의 차다. 두 시계가 어긋나면 그 차이가 지연 수치에 통째로 섞인다.
 * CDC 스택의 V1 1차 실행이 그랬다 — 편차 3,561ms 에 재려던 지연이 526ms 였으니 오차가 7배 컸다.
 * 편차를 같이 내야 "지연이 큰 것"과 "시계가 틀린 것"을 가를 수 있고, 그래야 지연 경보에
 * 게이트를 걸 수 있다.
 *
 * <p><b>왕복 시간 보정.</b> 질의 직전/직후의 로컬 시각 중간값을 DB 응답 시각과 견준다.
 * 보정하지 않으면 네트워크 왕복의 절반이 편차로 계상되어, 편차가 없는 환경에서도 수 ms 가
 * 계속 찍힌다.
 *
 * <p><b>왜 {@code @Scheduled} 가 아닌가.</b> 이 서비스에는 스케줄러가 없다({@link
 * dev.cdccustom.application.SyncRunner} 도 자기 스레드로 돈다). 프로브 하나 때문에
 * {@code @EnableScheduling} 을 켜서 스레드 풀을 하나 더 만드는 것보다 데몬 스레드 하나가 싸다.
 *
 * <p>지표 이름은 CDC 두 스택과 같은 {@code cdc_clock_skew_seconds} 다. 같은 쿼리로 세 스택을
 * 한 화면에서 견주기 위해서다.
 */
@Component
public class SourceClockSkewProbe {

    private static final Logger log = LoggerFactory.getLogger(SourceClockSkewProbe.class);

    /** 목표 지연 5초의 10%. 이보다 크면 그 구간 지연값은 판정에 쓰지 않는다. */
    private static final long SKEW_BUDGET_MS = 500;

    private final JdbcTemplate sourceJdbc;
    private final SyncMetrics metrics;
    private final SyncProperties props;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread thread;

    public SourceClockSkewProbe(@Qualifier("sourceJdbc") JdbcTemplate sourceJdbc,
                                SyncMetrics metrics,
                                SyncProperties props) {
        this.sourceJdbc = sourceJdbc;
        this.metrics = metrics;
        this.props = props;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        thread = new Thread(this::loop, "clock-skew-probe");
        thread.setDaemon(true);
        thread.start();
    }

    private void loop() {
        while (running.get()) {
            probe();
            try {
                Thread.sleep(props.clockSkewProbeIntervalMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running.set(false);
            }
        }
    }

    void probe() {
        try {
            long before = System.currentTimeMillis();
            Long dbNowMs = sourceJdbc.queryForObject(
                    "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint", Long.class);
            long after = System.currentTimeMillis();
            if (dbNowMs == null) {
                return;
            }

            long skewMs = dbNowMs - (before + after) / 2;
            metrics.clockSkew(skewMs);

            if (Math.abs(skewMs) > SKEW_BUDGET_MS) {
                log.warn("source DB 와 시계 편차 {} ms — 이 구간의 반영 지연 수치는 신뢰할 수 없다", skewMs);
            }
        } catch (Exception e) {
            // 못 잰 것을 0 으로 덮지 않는다. 0 으로 덮으면 "편차 없음"으로 읽혀 지연 경보의
            // 게이트가 열린 채로 남는다. 마지막 값이 그대로 있고, 못 재고 있다는 사실은 이 로그로 드러난다.
            log.warn("시계 편차 측정 실패 — 지연 지표의 신뢰도를 판정할 수 없다: {}", e.toString());
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (thread != null) {
            thread.interrupt();
        }
    }
}
