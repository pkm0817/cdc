package dev.embeddedcdc.application.handler;

import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.SourceTable;

/**
 * 테이블 하나를 target 에 반영하는 방법.
 *
 * 테이블을 추가하려면 이 인터페이스 구현 하나만 빈으로 등록하면 된다 —
 * ChangeEventService 는 고칠 필요가 없다. switch 문을 두지 않은 이유다.
 */
public interface TableSyncHandler {

    SourceTable table();

    /**
     * @return 반영된 행 수. <b>0 은 오류가 아니라 더 오래된 이벤트가 차단된 것</b>이다.
     *         DLQ 재처리는 이 값으로 "실제로 반영했다"와 "차단됐다"를 가른다 —
     *         예외 유무만 보면 둘이 똑같이 성공으로 기록된다.
     */
    int apply(ChangeEvent event);
}
