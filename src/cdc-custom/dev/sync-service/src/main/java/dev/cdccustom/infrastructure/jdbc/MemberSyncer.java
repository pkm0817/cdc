package dev.cdccustom.infrastructure.jdbc;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * member — grade 를 참조하는 자식 표.
 *
 * <p>소스에는 FK 가 있지만 타깃에는 없다. 수신 측에 같은 제약을 걸면 반영 순서가
 * 어긋나는 순간 연쇄 실패가 나기 때문이다(CDC 판과 같은 판단).
 */
@Component
class MemberSyncer extends AbstractJdbcSyncer {

    private static final String SELECT = """
            SELECT id, email, name, grade_id, point, created_at, updated_at
              FROM member
             WHERE id = ANY(?)
            """;

    private static final String UPSERT = """
            INSERT INTO member (id, email, name, grade_id, point, created_at, updated_at,
                                deleted, source_seq, synced_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, FALSE, ?, now())
            ON CONFLICT (id) DO UPDATE SET
                email      = EXCLUDED.email,
                name       = EXCLUDED.name,
                grade_id   = EXCLUDED.grade_id,
                point      = EXCLUDED.point,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                deleted    = FALSE,
                source_seq = EXCLUDED.source_seq,
                synced_at  = now()
            """;

    private static final String SOFT_DELETE = """
            UPDATE member
               SET deleted = TRUE, source_seq = ?, synced_at = now()
             WHERE id = ANY(?)
            """;

    MemberSyncer(@Qualifier("sourceJdbc") JdbcTemplate sourceJdbc,
                @Qualifier("targetJdbc") JdbcTemplate targetJdbc) {
        super(sourceJdbc, targetJdbc);
    }

    @Override
    public String table() {
        return "member";
    }

    @Override
    public int upsert(List<Long> ids, long seq) {
        if (ids.isEmpty()) {
            return 0;
        }
        List<Object[]> rows = readSource(SELECT, ids, (rs, i) -> new Object[]{
                rs.getLong("id"),
                rs.getString("email"),
                rs.getString("name"),
                rs.getLong("grade_id"),
                rs.getInt("point"),
                rs.getTimestamp("created_at"),
                rs.getTimestamp("updated_at"),
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
