package dev.cdccustom.infrastructure.jdbc;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * grade — member 가 참조하는 부모 표. 값은 그대로 옮기고 삭제는 표시만 한다.
 *
 * <p>타깃에는 FK 를 두지 않으므로 grade 와 member 의 반영 순서를 맞출 필요가 없다.
 * 부모가 늦게 와도 자식 적재가 막히지 않는다.
 */
@Component
class GradeSyncer extends AbstractJdbcSyncer {

    private static final String SELECT = """
            SELECT id, code, name, discount_rate, created_at
              FROM grade
             WHERE id = ANY(?)
            """;

    private static final String UPSERT = """
            INSERT INTO grade (id, code, name, discount_rate, created_at, deleted, source_seq, synced_at)
            VALUES (?, ?, ?, ?, ?, FALSE, ?, now())
            ON CONFLICT (id) DO UPDATE SET
                code          = EXCLUDED.code,
                name          = EXCLUDED.name,
                discount_rate = EXCLUDED.discount_rate,
                created_at    = EXCLUDED.created_at,
                deleted       = FALSE,
                source_seq    = EXCLUDED.source_seq,
                synced_at     = now()
            """;

    private static final String SOFT_DELETE = """
            UPDATE grade
               SET deleted = TRUE, source_seq = ?, synced_at = now()
             WHERE id = ANY(?)
            """;

    GradeSyncer(@Qualifier("sourceJdbc") JdbcTemplate sourceJdbc,
                @Qualifier("targetJdbc") JdbcTemplate targetJdbc) {
        super(sourceJdbc, targetJdbc);
    }

    @Override
    public String table() {
        return "grade";
    }

    @Override
    public int upsert(List<Long> ids, long seq) {
        if (ids.isEmpty()) {
            return 0;
        }
        List<Object[]> rows = readSource(SELECT, ids, (rs, i) -> new Object[]{
                rs.getLong("id"),
                rs.getString("code"),
                rs.getString("name"),
                rs.getBigDecimal("discount_rate"),
                rs.getTimestamp("created_at"),
                seq
        });
        if (rows.isEmpty()) {
            return 0;
        }
        target.batchUpdate(UPSERT, rows);
        return rows.size();
    }

    @Override
    public int delete(List<Long> ids, long seq) {
        return applyByIds(SOFT_DELETE, ids, seq);
    }
}
