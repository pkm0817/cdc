package dev.embeddedcdc.domain.port.out;

/**
 * 파이프라인 관측 지표(outbound port).
 *
 * 구현체가 노출하는 이름은 Grafana 대시보드가 그대로 참조하므로 바꾸면 대시보드가 깨진다.
 *   cdc_events_total{table, op}
 *   cdc_sink_errors_total{table}
 *   cdc_end_to_end_lag_seconds{table}
 *   cdc_capture_gap
 *   cdc_clock_skew_seconds
 *   cdc_change_audit_rows_total{table}
 *   cdc_pipeline_halts_total{reason}
 */
public interface PipelineMetrics {

    void eventApplied(String table, String op);

    void applyFailed(String table);

    /** DLQ 로 격리된 건. 0 이 아니면 미반영 데이터가 쌓이고 있다는 뜻이다. */
    void deadLettered(String table);

    /** @param sourceCommittedAtMs source 커밋 시각. 0 이하면 기록하지 않는다 */
    void endToEndLag(String table, long sourceCommittedAtMs);

    /**
     * 기동 시 캡처 갭을 발견했는지. 1 이면 엔진을 띄우지 않았다는 뜻이다.
     *
     * Counter 가 아니라 Gauge 인 이유: 갭은 기동 시점에 한 번 정해지고 그 상태가
     * 계속 노출돼야 경보가 걸린다. 한 번 올리고 마는 카운터로는 재기동 뒤 값이 사라진다.
     */
    void captureGap(boolean detected);

    /**
     * 파이프라인이 멈춘 횟수.
     *
     * reason 은 반드시 고정 집합이어야 한다 — 예외 메시지를 그대로 넣으면
     * 카디널리티가 터진다 (V1 에서 변경 필드명을 레이블로 안 쓴 것과 같은 이유).
     */
    void pipelineHalted(String reason);

    /**
     * source DB 시계에서 이 프로세스 시계를 뺀 값(밀리초).
     *
     * end-to-end 지연은 source 의 ts_ms 와 이쪽 벽시계의 차이로 계산한다.
     * 두 시계가 어긋나 있으면 그 차이가 지연 수치에 그대로 섞여 들어간다 —
     * V1 1차 실행에서 편차 3,561ms 에 지연 526ms 가 나와 지연값을 못 쓴 적이 있다.
     * 지연 지표를 믿어도 되는지 판정하려면 편차를 같이 노출해야 한다.
     */
    void clockSkew(long skewMs);

    /** 필드 단위 변경 이력을 남긴 건수. 필드명은 레이블로 올리지 않는다 (카디널리티). */
    void changeAudited(String table);
}
