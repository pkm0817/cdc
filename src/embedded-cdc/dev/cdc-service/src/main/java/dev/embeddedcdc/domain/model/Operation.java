package dev.embeddedcdc.domain.model;

import java.util.Optional;

/**
 * 변경 이벤트의 종류. Debezium 의 op 코드에 대응한다.
 *
 * SNAPSHOT_READ 와 CREATE 를 굳이 나눠 두지만 처리는 같다 —
 * 둘 다 멱등 UPSERT 로 흘려보내야 오프셋을 잃고 스냅샷이 다시 돌아도 target 이 깨지지 않는다.
 * 지표에서 "스냅샷이 얼마나 돌았는지"를 구분해 보기 위해 코드만 남겨 둔다.
 */
public enum Operation {

    SNAPSHOT_READ("r"),
    CREATE("c"),
    UPDATE("u"),
    DELETE("d");

    private final String code;

    Operation(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }

    /** 이 파이프라인의 관심 밖인 op(t=truncate, m=message 등)는 empty 를 돌려준다. */
    public static Optional<Operation> fromCode(String code) {
        for (Operation op : values()) {
            if (op.code.equals(code)) {
                return Optional.of(op);
            }
        }
        return Optional.empty();
    }

    /** DELETE 를 제외한 나머지는 전부 UPSERT 경로를 탄다. */
    public boolean isUpsert() {
        return this != DELETE;
    }
}
