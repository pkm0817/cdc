package dev.embeddedcdc.domain.port.out;

import dev.embeddedcdc.domain.model.Computer;

/**
 * target 의 computer 저장소(outbound port).
 *
 * car 와 달리 두 메서드 모두 "더 새로운 이벤트일 때만" 반영해야 한다.
 * 순서 역전 방어를 구현체가 아니라 이 계약에 못 박아 둔다 —
 * 나중에 저장소를 갈아 끼워도 이 성질이 사라지지 않게 하기 위해서다.
 */
public interface ComputerRepository {

    /**
     * 저장된 source_lsn 보다 새로운 이벤트일 때만 반영한다.
     *
     * @return 실제로 반영된 행 수. 0 이면 더 오래된 이벤트라 차단된 것이다
     */
    int upsertIfNewer(Computer computer);

    /**
     * 물리 삭제가 아니라 deleted 플래그를 세운다.
     * 물리 삭제하면 늦게 도착한 UPDATE 가 행을 되살려 유령 데이터가 남는다.
     *
     * @return 실제로 반영된 행 수
     */
    int softDelete(long id, long lsn);
}
