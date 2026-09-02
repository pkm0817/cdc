package dev.cdccustom.domain;

/**
 * outbox 에 적히는 연산. 값은 소스 트리거가 쓰는 문자와 같다.
 *
 * <p>CDC 의 op 코드(c/u/d/r)와 일부러 맞췄다. 세 스택의 지표를 같은 라벨로 묶어
 * 대시보드 하나에서 비교하기 위해서다. 다만 여기에는 스냅샷을 뜻하는 r 이 없다 —
 * 초기 적재도 결국 "그 시점의 현재 값을 읽어 쓰는" 같은 경로라 c 로 기록한다.
 */
public enum Op {
    CREATE("c"),
    UPDATE("u"),
    DELETE("d");

    private final String code;

    Op(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }

    public static Op from(String code) {
        return switch (code) {
            case "c" -> CREATE;
            case "u" -> UPDATE;
            case "d" -> DELETE;
            default -> throw new IllegalArgumentException("알 수 없는 op: " + code);
        };
    }

    /**
     * 이 연산이 "소스에서 현재 값을 읽어 반영"에 해당하는가.
     * 삭제만 아니면 전부 그렇다 — 생성과 수정을 구분할 필요가 없다.
     * 어차피 UPSERT 한 문장으로 처리하기 때문이다.
     */
    public boolean isUpsert() {
        return this != DELETE;
    }
}
