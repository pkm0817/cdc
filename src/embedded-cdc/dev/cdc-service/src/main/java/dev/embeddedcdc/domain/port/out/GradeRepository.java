package dev.embeddedcdc.domain.port.out;

import dev.embeddedcdc.domain.model.Grade;

/**
 * target 의 grade 저장소(outbound port).
 *
 * ComputerRepository 와 같은 계약이다 — 두 연산 모두 "더 새로운 이벤트일 때만" 반영한다.
 *
 * <b>삭제가 소프트인 것은 여기서는 선택이 아니라 필수다.</b>
 * member 가 grade_id 로 이 행을 가리키고 있다. 물리 삭제하면 남아 있는 member 의
 * grade_id 가 어디도 가리키지 못하는 값이 되어, 나중에 등급명을 되짚을 수 없다.
 * (target 에 FK 를 걸지 않았으므로 DB 가 막아 주지도 않는다)
 */
public interface GradeRepository {

    /**
     * 저장된 source_lsn 보다 새로운 이벤트일 때만 반영한다.
     *
     * @return 실제로 반영된 행 수. 0 이면 더 오래된 이벤트라 차단된 것이다
     */
    int upsertIfNewer(Grade grade);

    /**
     * 물리 삭제가 아니라 deleted 플래그를 세운다.
     *
     * @return 실제로 반영된 행 수
     */
    int softDelete(long id, long lsn);
}
