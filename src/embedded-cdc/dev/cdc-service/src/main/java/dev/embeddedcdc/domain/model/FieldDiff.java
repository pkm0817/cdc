package dev.embeddedcdc.domain.model;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * 한 UPDATE 에서 "무엇이 바뀌었는지"의 판정 결과.
 *
 * V1 의 통과 기준 절반이 이것이다 — 이벤트가 제때 오는 것만으로는 부족하고,
 * 어떤 필드가 무엇에서 무엇으로 바뀌었는지 말할 수 있어야 한다.
 *
 * 판정은 before/after 비교 하나뿐이라 <b>REPLICA IDENTITY FULL 이 전제</b>다.
 * DEFAULT 면 before 에 PK 만 실려 비교 자체가 성립하지 않는다 — 그 경우를
 * "변경 없음"으로 뭉개지 않고 {@code identifiable=false} 로 구분해 돌려준다.
 * 둘을 같은 값으로 돌려주면 "안 바뀐 것"과 "모르는 것"이 감사 로그에서 섞인다.
 *
 * 검증 테스트와 운영 코드가 이 클래스를 같이 쓴다. 판정식이 갈라지면
 * 검증에서 통과한 것과 운영에서 기록되는 것이 서로 다른 것이 된다.
 *
 * @param identifiable 필드 단위 판정이 가능한 이벤트였는지 (before/after 가 둘 다 있는지)
 * @param changed      실제로 값이 달라진 컬럼
 * @param unreadable   값을 읽을 수 없어 판정에서 제외한 컬럼 (TOAST 자리표시자)
 */
public record FieldDiff(boolean identifiable, Set<String> changed, Set<String> unreadable) {

    /**
     * Debezium 이 값을 실을 수 없을 때 채워 넣는 자리표시자.
     * UPDATE 에서 건드리지 않은 대용량(TOAST) 컬럼이 이 값으로 온다 (V5).
     */
    public static final String UNAVAILABLE = "__debezium_unavailable_value";

    private static final FieldDiff NOT_IDENTIFIABLE =
            new FieldDiff(false, Set.of(), Set.of());

    public FieldDiff {
        changed = Collections.unmodifiableSet(new TreeSet<>(changed));
        unreadable = Collections.unmodifiableSet(new TreeSet<>(unreadable));
    }

    /**
     * before 와 after 를 견줘 바뀐 필드만 뽑는다.
     * 한쪽이라도 없으면(INSERT/DELETE, 또는 REPLICA IDENTITY DEFAULT) 판정 불가로 돌려준다.
     */
    public static FieldDiff between(RowData before, RowData after) {
        if (before == null || after == null) {
            return NOT_IDENTIFIABLE;
        }

        Set<String> changed = new TreeSet<>();
        Set<String> unreadable = new TreeSet<>();

        Set<String> columns = new LinkedHashSet<>(before.values().keySet());
        columns.addAll(after.values().keySet());

        for (String column : columns) {
            String b = before.values().get(column);
            String a = after.values().get(column);

            // 자리표시자는 "값이 바뀌었다"가 아니라 "값을 못 읽었다"다.
            // 그냥 비교하면 안 건드린 대용량 컬럼이 매번 변경으로 잡혀 감사 로그가 거짓말을 한다.
            if (UNAVAILABLE.equals(a) || UNAVAILABLE.equals(b)) {
                unreadable.add(column);
                continue;
            }
            if (b == null ? a != null : !b.equals(a)) {
                changed.add(column);
            }
        }
        return new FieldDiff(true, changed, unreadable);
    }

    public boolean hasChanges() {
        return !changed.isEmpty();
    }
}
