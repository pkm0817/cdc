package dev.cdccustom.domain.port;

import java.util.List;

/**
 * 표 하나를 소스에서 타깃으로 맞추는 담당자.
 *
 * <p>CDC 판의 {@code TableSyncHandler} 와 역할이 같지만 입력이 다르다.
 * CDC 판은 <b>이벤트(값을 담은)</b> 를 받고, 여기서는 <b>PK 목록</b> 만 받는다.
 * 값을 어디서 가져올지는 구현이 정한다 — 전부 소스에서 다시 읽는다.
 */
public interface TableSyncer {

    /** 담당하는 소스 테이블 이름. outbox 의 table_name 과 일치해야 한다. */
    String table();

    /**
     * 주어진 PK 들의 현재 값을 소스에서 읽어 타깃에 UPSERT 한다.
     *
     * <p>읽는 시점에 소스에 없는 PK 는 조용히 건너뛴다. 그 사이 삭제된 행이라는 뜻이고,
     * 그 삭제는 다음 배치의 outbox 에 이미 들어와 있다. 여기서 억지로 지우면
     * 아직 처리하지 않은 순번을 앞질러 처리하는 셈이 된다.
     *
     * @param seq 이 배치의 최대 seq — 타깃에 관측용으로 남긴다
     * @return 실제로 쓴 행 수
     */
    int upsert(List<Long> ids, long seq);

    /**
     * 주어진 PK 들을 타깃에서 지운다.
     *
     * <p>스키마에 소프트 삭제 컬럼이 있는 표는 표시만 하고 행은 남긴다.
     * 물리 삭제하면 늦게 도착한 갱신이 행을 되살릴 수 있기 때문인데,
     * 이 방식에서는 순서 역전이 없으므로 그 이유는 사라졌다.
     * 그럼에도 남기는 것은 CDC 판과 타깃 스키마·조회 결과를 맞추기 위해서다.
     *
     * @return 실제로 지운(표시한) 행 수
     */
    int delete(List<Long> ids, long seq);
}
