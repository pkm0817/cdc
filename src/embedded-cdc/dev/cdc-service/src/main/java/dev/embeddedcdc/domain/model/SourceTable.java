package dev.embeddedcdc.domain.model;

import java.util.Optional;

/**
 * 이 파이프라인이 동기화하는 source 테이블.
 *
 * 여기에 없는 테이블의 이벤트는 무시된다. publication 이 두 테이블만 담고 있으므로
 * 평소에는 도달하지 않지만, publication 이 바뀌면 조용히 유실되는 대신 경고가 남도록
 * 이름을 열거형으로 고정해 둔다.
 */
public enum SourceTable {

    CAR("car"),
    COMPUTER("computer"),

    // grade 는 member 의 부모다. 열거 순서는 적용 순서와 무관하다 —
    // 순서는 이벤트의 LSN 이 정하고, BatchApplier 가 그 순서대로 부른다.
    GRADE("grade"),
    MEMBER("member");

    private final String tableName;

    SourceTable(String tableName) {
        this.tableName = tableName;
    }

    public String tableName() {
        return tableName;
    }

    public static Optional<SourceTable> fromName(String name) {
        for (SourceTable table : values()) {
            if (table.tableName.equals(name)) {
                return Optional.of(table);
            }
        }
        return Optional.empty();
    }
}
