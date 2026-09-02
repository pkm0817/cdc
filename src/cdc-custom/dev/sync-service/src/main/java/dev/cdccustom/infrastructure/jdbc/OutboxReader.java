package dev.cdccustom.infrastructure.jdbc;

import dev.cdccustom.domain.Op;
import dev.cdccustom.domain.OutboxEntry;
import dev.cdccustom.domain.PendingChanges;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 소스의 sync_outbox 를 읽고, 반영이 끝난 구간을 비운다.
 *
 * <p>읽기는 {@code seq > 체크포인트} 를 순번대로 훑는 단순 조회다. PK 인덱스를 그대로
 * 타므로 표가 커져도 훑는 양은 배치 크기에 비례한다.
 */
@Component
public class OutboxReader {

    private static final String SELECT = """
            SELECT seq, table_name, row_id, op, changed_at
              FROM sync_outbox
             WHERE seq > ?
             ORDER BY seq
             LIMIT ?
            """;

    /** 반영이 끝난 구간을 비운다. 비우지 않으면 outbox 가 무한히 커진다. */
    private static final String PRUNE = "DELETE FROM sync_outbox WHERE seq <= ?";

    /** 아직 반영되지 않은 줄 수 — CDC 의 슬롯 지연에 해당하는 지표다. */
    private static final String PENDING = "SELECT count(*) FROM sync_outbox WHERE seq > ?";

    private final JdbcTemplate source;

    public OutboxReader(@Qualifier("sourceJdbc") JdbcTemplate sourceJdbc) {
        this.source = sourceJdbc;
    }

    /**
     * 체크포인트 다음부터 최대 {@code limit} 줄을 읽어 중복을 접은 결과로 돌려준다.
     *
     * <p>읽으면서 바로 접는다. 5,000줄을 리스트로 들고 있다가 접는 것과 결과는 같지만
     * 메모리에 남는 것은 접힌 결과뿐이다 — 같은 행을 계속 갱신하는 부하에서 차이가 크다.
     */
    public PendingChanges readBatch(long afterSeq, int limit) {
        PendingChanges changes = new PendingChanges();
        source.query(SELECT, rs -> {
            changes.add(new OutboxEntry(
                    rs.getLong("seq"),
                    rs.getString("table_name"),
                    rs.getLong("row_id"),
                    Op.from(rs.getString("op").trim()),
                    rs.getTimestamp("changed_at").toInstant()));
        }, afterSeq, limit);
        return changes;
    }

    public void prune(long upToSeq) {
        source.update(PRUNE, upToSeq);
    }

    public long pending(long afterSeq) {
        Long n = source.queryForObject(PENDING, Long.class, afterSeq);
        return n == null ? 0 : n;
    }
}
