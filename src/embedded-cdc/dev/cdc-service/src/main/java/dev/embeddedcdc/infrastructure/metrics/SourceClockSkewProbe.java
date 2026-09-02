package dev.embeddedcdc.infrastructure.metrics;

import dev.embeddedcdc.domain.port.out.PipelineMetrics;
import dev.embeddedcdc.infrastructure.config.CdcSourceProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * source DB 와 이 프로세스의 시계 차이를 주기적으로 잰다.
 *
 * <b>왜 필요한가.</b> cdc_end_to_end_lag_seconds 는 source 이벤트의 ts_ms(DB 시계)와
 * 적재 시각(앱 시계)의 차이다. 두 시계가 어긋나면 그 차이가 지연 수치에 통째로 섞인다.
 * V1 1차 실행이 그랬다 — 편차 3,561ms 에 지연 526ms 였으니 재려던 값보다 오차가 7배 컸다.
 * 편차를 같이 노출해야 "지연이 큰 것"과 "시계가 틀린 것"을 구분할 수 있다.
 *
 * <b>왜 별도 커넥션인가.</b> Spring 의 DataSource 는 target 을 가리킨다. 여기서 알아야 할
 * 것은 source 시계이므로 cdc.source.* 설정으로 직접 연다. 30초에 한 번 여닫는 커넥션이라
 * 풀을 하나 더 두는 것보다 이쪽이 싸고, 실패해도 파이프라인에 영향이 없다.
 *
 * <b>왕복 시간 보정.</b> 질의 직전/직후의 로컬 시각 중간값을 DB 응답 시각과 견준다.
 * 보정하지 않으면 네트워크 왕복의 절반이 편차로 계상되어, 편차가 없는 환경에서도
 * 수 ms 가 계속 찍힌다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SourceClockSkewProbe {

    private final CdcSourceProperties props;
    private final PipelineMetrics metrics;

    @Scheduled(fixedDelayString = "${cdc.source.clock-skew-probe-interval-ms:30000}",
               initialDelayString = "${cdc.source.clock-skew-probe-interval-ms:30000}")
    public void probe() {
        String url = "jdbc:postgresql://" + props.hostname() + ":" + props.port() + "/" + props.dbname();
        try (Connection connection = DriverManager.getConnection(url, props.user(), props.password());
             Statement statement = connection.createStatement()) {

            long before = System.currentTimeMillis();
            long dbNowMs;
            try (ResultSet rs = statement.executeQuery(
                    "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint")) {
                if (!rs.next()) {
                    return;
                }
                dbNowMs = rs.getLong(1);
            }
            long after = System.currentTimeMillis();

            long skewMs = dbNowMs - (before + after) / 2;
            metrics.clockSkew(skewMs);

            if (Math.abs(skewMs) > 500) {
                log.warn("source DB 와 시계 편차 {} ms — 이 구간의 end-to-end 지연 수치는 신뢰할 수 없다", skewMs);
            }
        } catch (Exception e) {
            // 못 잰 것을 0 으로 덮지 않는다. 마지막 값이 남고, 못 재고 있다는 사실은 이 로그로 드러난다.
            log.warn("시계 편차 측정 실패 — 지연 지표의 신뢰도를 판정할 수 없다: {}", e.toString());
        }
    }
}
