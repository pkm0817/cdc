package dev.embeddedcdc.domain.port.out;

import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.FieldDiff;

/**
 * 필드 단위 변경 이력을 남기는 곳(outbound port).
 *
 * <b>왜 메트릭이 아니라 표인가.</b> "어떤 필드가 바뀌었는지"를 Prometheus 레이블로 만들면
 * 시계열이 (테이블 × 컬럼 조합) 수만큼 생긴다. 컬럼 20개짜리 테이블 하나로
 * 조합이 백만 단위가 되고, 그 순간 Prometheus 가 먼저 죽는다.
 * 변경 필드는 카디널리티가 높은 <b>데이터</b>이지 지표 축이 아니다.
 * 지표로는 "몇 건 기록했는가"(cdc_change_audit_rows_total{table})만 낸다.
 *
 * <b>적용과 같은 트랜잭션에 둔다.</b> REQUIRES_NEW 로 떼어내면 적용이 롤백된 뒤에도
 * 감사 로그에는 반영된 것으로 남아, 감사 로그가 실제 target 상태와 어긋난다.
 * DLQ 와 반대되는 선택인데, DLQ 는 "실패했다는 사실"을 남기는 것이라 적용의
 * 롤백에 휩쓸리면 안 되고, 감사 로그는 "반영했다는 사실"이라 같이 롤백돼야 한다.
 */
public interface ChangeAuditStore {

    /**
     * UPDATE 한 건의 변경 필드를 남긴다.
     *
     * @param diff {@link FieldDiff#identifiable()} 가 false 면 판정 불가로 기록한다 —
     *             "안 바뀜"과 구분되어야 REPLICA IDENTITY 설정 사고를 사후에 찾을 수 있다
     */
    void record(String pipeline, ChangeEvent event, FieldDiff diff);
}
