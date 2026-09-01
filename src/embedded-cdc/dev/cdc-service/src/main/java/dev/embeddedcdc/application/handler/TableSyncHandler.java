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

    void apply(ChangeEvent event);
}
