package dev.embeddedcdc.domain.port.out;

import dev.embeddedcdc.domain.model.Member;

/**
 * target 의 member 저장소(outbound port).
 *
 * grade 를 참조하지만 이 계약에는 그 사실이 드러나지 않는다 —
 * grade_id 는 검증 없이 그대로 적재된다. 부모가 아직 안 왔는지 확인하지 않는 이유는
 * 확인해 봐야 할 수 있는 일이 "실패시키기"뿐이고, 그건 유실이 아니라 지연일 뿐인
 * 상황을 DLQ 로 밀어 넣기 때문이다. 순서는 스트림이 보장하고, 어긋남은 대사가 잡는다.
 */
public interface MemberRepository {

    /**
     * 저장된 source_lsn 보다 새로운 이벤트일 때만 반영한다.
     *
     * @return 실제로 반영된 행 수. 0 이면 더 오래된 이벤트라 차단된 것이다
     */
    int upsertIfNewer(Member member);

    /**
     * 물리 삭제가 아니라 deleted 플래그를 세운다.
     * 물리 삭제하면 늦게 도착한 UPDATE 가 행을 되살려 유령 데이터가 남는다.
     *
     * @return 실제로 반영된 행 수
     */
    int softDelete(long id, long lsn);
}
