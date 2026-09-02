package dev.cdccustom.infrastructure.jdbc;

import dev.cdccustom.domain.port.TableSyncer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;

/**
 * 표별 구현이 공통으로 쓰는 뼈대.
 *
 * <p>두 가지를 공통화한다.
 * <ol>
 *   <li><b>PK 목록으로 한 번에 읽기</b> — {@code WHERE id = ANY(?)} 로 왕복 한 번에 끝낸다.
 *       PK 하나씩 조회하면 배치 5,000건이 곧 왕복 5,000번이 되어, 이 방식의 이점이 사라진다.</li>
 *   <li><b>배치 쓰기</b> — JDBC 배치로 모아 보낸다. 커넥션 URL 에
 *       {@code reWriteBatchedInserts=true} 를 켜 두었으므로 드라이버가 이를 다중 VALUES
 *       INSERT 로 다시 써서 실제 왕복은 한 번에 가깝다.</li>
 * </ol>
 */
abstract class AbstractJdbcSyncer implements TableSyncer {

    protected final JdbcTemplate source;
    protected final JdbcTemplate target;

    protected AbstractJdbcSyncer(JdbcTemplate source, JdbcTemplate target) {
        this.source = source;
        this.target = target;
    }

    /**
     * 소스에서 PK 목록에 해당하는 현재 행을 읽는다.
     *
     * <p>{@code = ANY(?)} 에는 java.sql.Array 가 필요하다. setObject 로 List 를 넘기면
     * 드라이버가 배열로 바꿔주지 않아 타입 오류가 난다 — 그래서 커넥션에서 배열을 만든다.
     */
    protected <T> List<T> readSource(String sql, List<Long> ids, RowMapper<T> mapper) {
        return source.execute((org.springframework.jdbc.core.ConnectionCallback<List<T>>) conn -> {
            try (var ps = conn.prepareStatement(sql)) {
                ps.setArray(1, conn.createArrayOf("bigint", ids.toArray(new Long[0])));
                try (var rs = ps.executeQuery()) {
                    List<T> rows = new java.util.ArrayList<>(ids.size());
                    int rowNum = 0;
                    while (rs.next()) {
                        rows.add(mapper.mapRow(rs, rowNum++));
                    }
                    return rows;
                }
            }
        });
    }

    /** 타깃에서 PK 목록을 한 문장으로 처리한다(물리 삭제 · 소프트 삭제 공통). */
    protected int applyByIds(String sql, List<Long> ids, Long seq) {
        if (ids.isEmpty()) {
            return 0;
        }
        Integer affected = target.execute((org.springframework.jdbc.core.ConnectionCallback<Integer>) conn -> {
            try (var ps = conn.prepareStatement(sql)) {
                int idx = 1;
                if (seq != null) {
                    ps.setLong(idx++, seq);
                }
                ps.setArray(idx, conn.createArrayOf("bigint", ids.toArray(new Long[0])));
                return ps.executeUpdate();
            }
        });
        return affected == null ? 0 : affected;
    }
}
