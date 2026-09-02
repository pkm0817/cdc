package dev.cdccustom.infrastructure.jdbc;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * computer — source 와 target 스키마가 다른 경우. 이 파이프라인의 유일한 업무 규칙이다.
 *
 * <pre>
 *   brand + model      -> full_name
 *   cpu + ram_gb       -> spec
 *   price_usd x 1350   -> price_krw
 * </pre>
 *
 * <p>CDC 판({@code ComputerMapper})과 규칙·환율을 똑같이 맞췄다. 세 스택의 타깃을
 * 같은 쿼리로 대조하려면 변환 결과가 한 글자도 달라선 안 된다.
 */
@Component
class ComputerSyncer extends AbstractJdbcSyncer {

    /** 데모용 고정 환율. 실제라면 시점별 환율이 필요하므로 별도 포트로 나가야 한다. */
    private static final BigDecimal USD_TO_KRW = new BigDecimal("1350");

    private static final String SELECT = """
            SELECT id, brand, model, cpu, ram_gb, price_usd
              FROM computer
             WHERE id = ANY(?)
            """;

    private static final String UPSERT = """
            INSERT INTO computer (id, full_name, spec, price_krw, deleted, source_seq, synced_at)
            VALUES (?, ?, ?, ?, FALSE, ?, now())
            ON CONFLICT (id) DO UPDATE SET
                full_name  = EXCLUDED.full_name,
                spec       = EXCLUDED.spec,
                price_krw  = EXCLUDED.price_krw,
                deleted    = FALSE,
                source_seq = EXCLUDED.source_seq,
                synced_at  = now()
            """;

    private static final String SOFT_DELETE = """
            UPDATE computer
               SET deleted = TRUE, source_seq = ?, synced_at = now()
             WHERE id = ANY(?)
            """;

    ComputerSyncer(@Qualifier("sourceJdbc") JdbcTemplate sourceJdbc,
                @Qualifier("targetJdbc") JdbcTemplate targetJdbc) {
        super(sourceJdbc, targetJdbc);
    }

    @Override
    public String table() {
        return "computer";
    }

    @Override
    public int upsert(List<Long> ids, long seq) {
        if (ids.isEmpty()) {
            return 0;
        }
        List<Object[]> rows = readSource(SELECT, ids, (rs, i) -> new Object[]{
                rs.getLong("id"),
                rs.getString("brand") + " " + rs.getString("model"),
                rs.getString("cpu") + " / " + rs.getInt("ram_gb") + "GB",
                rs.getBigDecimal("price_usd").multiply(USD_TO_KRW).setScale(0, RoundingMode.HALF_UP),
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
