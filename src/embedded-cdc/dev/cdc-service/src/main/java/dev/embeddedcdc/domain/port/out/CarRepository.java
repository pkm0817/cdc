package dev.embeddedcdc.domain.port.out;

import dev.embeddedcdc.domain.model.Car;

/**
 * target 의 car 저장소(outbound port).
 *
 * <b>두 연산 모두 "더 새로운 이벤트일 때만" 반영된다.</b> 멱등만으로는 모자라기 때문이다 —
 * 같은 이벤트가 두 번 오는 것(중복)은 멱등이 막지만, 오래된 이벤트가 뒤늦게 오는 것(역전)은
 * 막지 못한다. 뒤엣것이 실제로 데이터를 깬다: DLQ 재처리는 격리된 뒤 시간이 지나 적용되므로
 * 그 사이 정상 경로로 더 새로운 값이 들어와 있을 수 있다.
 *
 * 반환하는 int 는 반영된 행 수다. <b>0 은 오류가 아니라 더 오래된 이벤트가 차단된 것</b>이며,
 * 호출부는 이 값으로 "반영됨"과 "차단됨"을 갈라야 한다.
 */
public interface CarRepository {

    int upsertIfNewer(Car car);

    /**
     * car 는 소프트 삭제를 쓰지 않는다(하드 삭제 테이블이다). 대신 저장된 LSN 보다
     * 새로운 이벤트일 때만 지워 순서 역전을 막는다.
     */
    int deleteIfNewer(long id, long lsn);
}
