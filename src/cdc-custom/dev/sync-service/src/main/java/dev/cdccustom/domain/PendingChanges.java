package dev.cdccustom.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * outbox 배치를 "표별 반영 목록"으로 접은 결과.
 *
 * <p>순수 자료구조다. DB 도 스프링도 모르므로 단위 테스트로 접기 규칙만 따로 검증할 수 있다.
 *
 * <p>접는 규칙은 하나다 — <b>같은 (표, 행)이면 마지막 op 만 남긴다.</b>
 * <ul>
 *   <li>c → u → u : upsert 한 번 (최종 값을 읽어 쓴다)</li>
 *   <li>c → d     : delete 한 번 (생성했다 지운 행을 굳이 만들 필요가 없다)</li>
 *   <li>d → c     : upsert 한 번 (같은 PK 가 되살아난 경우. 소스의 현재 값이 정답이다)</li>
 * </ul>
 */
public final class PendingChanges {

    private final Map<ChangeKey, Op> latest = new LinkedHashMap<>();
    private final Map<String, Instant> oldestByTable = new LinkedHashMap<>();
    private long maxSeq;
    private int rawCount;

    public void add(OutboxEntry entry) {
        latest.put(entry.key(), entry.op());   // 같은 키면 뒤엣것이 이긴다
        rawCount++;
        if (entry.seq() > maxSeq) {
            maxSeq = entry.seq();
        }
        // 지연은 표별로 잰다. 배치 전체의 최솟값 하나만 두면 어느 표가 밀렸는지 알 수 없다.
        oldestByTable.merge(entry.table(), entry.changedAt(),
                (existing, candidate) -> candidate.isBefore(existing) ? candidate : existing);
    }

    /** 이 배치에 등장한 표 목록 — 반영과 지연 기록 대상이다. */
    public java.util.Set<String> tables() {
        return oldestByTable.keySet();
    }

    /** 표별로 가장 오래된 변경 시각. 지연 지표의 기준점이다. */
    public Instant oldestChange(String table) {
        return oldestByTable.get(table);
    }

    public boolean isEmpty() {
        return latest.isEmpty();
    }

    /** 접기 전 줄 수 — 접힌 뒤와 비교하면 이 방식이 아낀 양이 그대로 보인다. */
    public int rawCount() {
        return rawCount;
    }

    /** 접은 뒤 실제로 타깃에 쓸 행 수. */
    public int foldedCount() {
        return latest.size();
    }

    /** 이 배치를 반영하고 나면 체크포인트가 가리킬 seq. */
    public long maxSeq() {
        return maxSeq;
    }

    /** 표별로 UPSERT 할 PK 목록. */
    public List<Long> upsertIds(String table) {
        return idsWhere(table, true);
    }

    /** 표별로 DELETE 할 PK 목록. */
    public List<Long> deleteIds(String table) {
        return idsWhere(table, false);
    }

    private List<Long> idsWhere(String table, boolean upsert) {
        List<Long> ids = new ArrayList<>();
        for (Map.Entry<ChangeKey, Op> e : latest.entrySet()) {
            if (e.getKey().table().equals(table) && e.getValue().isUpsert() == upsert) {
                ids.add(e.getKey().rowId());
            }
        }
        return ids;
    }
}
