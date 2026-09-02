package dev.cdccustom.infrastructure.jdbc;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * car — source 와 target 스키마가 같은 경우. 변환 없이 그대로 옮긴다.
 *
 * <p>타깃 car 에는 소프트 삭제 컬럼이 없다(CDC 판과 동일). 그래서 삭제는 물리 삭제다.
 */
@Component
class CarSyncer extends AbstractJdbcSyncer {

    private static final String SELECT = """
            SELECT id, name, brand, price, created_at, updated_at
              FROM car
             WHERE id = ANY(?)
            """;

    private static final String UPSERT = """
            INSERT INTO car (id, name, brand, price, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                name       = EXCLUDED.name,
                brand      = EXCLUDED.brand,
                price      = EXCLUDED.price,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at
            """;

    private static final String DELETE = "DELETE FROM car WHERE id = ANY(?)";

    CarSyncer(@Qualifier("sourceJdbc") JdbcTemplate sourceJdbc,
                @Qualifier("targetJdbc") JdbcTemplate targetJdbc) {
        super(sourceJdbc, targetJdbc);
    }

    @Override
    public String table() {
        return "car";
    }

    @Override
    public int upsert(List<Long> ids, long seq) {
        if (ids.isEmpty()) {
            return 0;
        }
        List<Object[]> rows = readSource(SELECT, ids, (rs, i) -> new Object[]{
                rs.getLong("id"),
                rs.getString("name"),
                rs.getString("brand"),
                rs.getBigDecimal("price"),
                rs.getTimestamp("created_at"),
                rs.getTimestamp("updated_at")
        });
        if (rows.isEmpty()) {
            return 0;
        }
        target.batchUpdate(UPSERT, rows);
        return rows.size();
    }

    @Override
    public int delete(List<Long> ids, long seq) {
        return applyByIds(DELETE, ids, null);
    }
}
