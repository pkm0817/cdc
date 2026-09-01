package dev.embeddedcdc.domain.port.out;

/**
 * 파이프라인 관측 지표(outbound port).
 *
 * 구현체가 노출하는 이름은 Grafana 대시보드가 그대로 참조하므로 바꾸면 대시보드가 깨진다.
 *   cdc_events_total{table, op}
 *   cdc_sink_errors_total{table}
 *   cdc_end_to_end_lag_seconds{table}
 */
public interface PipelineMetrics {

    void eventApplied(String table, String op);

    void applyFailed(String table);

    /** DLQ 로 격리된 건. 0 이 아니면 미반영 데이터가 쌓이고 있다는 뜻이다. */
    void deadLettered(String table);

    /** @param sourceCommittedAtMs source 커밋 시각. 0 이하면 기록하지 않는다 */
    void endToEndLag(String table, long sourceCommittedAtMs);
}
