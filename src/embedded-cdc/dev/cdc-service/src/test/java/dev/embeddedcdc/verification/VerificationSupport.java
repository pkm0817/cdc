package dev.embeddedcdc.verification;

import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.RowData;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * 검증 테스트 공통 준비물.
 *
 * 캡처 대상은 운영 테이블(car, computer)이 아니라 검증 전용 테이블을 쓴다.
 * 기동 중인 emb-cdc-service 의 publication·슬롯과 겹치면 서로의 결과를 오염시킨다.
 */
public abstract class VerificationSupport {

    protected static final String RECORD_TABLE = "verify_record";
    protected static final String RECORD_PUBLICATION = "verify_record_pub";

    /**
     * 실적 레코드를 본뜬 검증 테이블.
     * payload 는 TOAST 임계(약 2KB)를 넘길 수 있는 대용량 필드로, V5 에서 쓴다.
     */
    protected static void createRecordFixture() {
        Db.onSource("""
                CREATE TABLE IF NOT EXISTS verify_record (
                    id          BIGSERIAL     PRIMARY KEY,
                    biz_key     TEXT          NOT NULL,
                    amount      NUMERIC(14,2) NOT NULL,
                    status      TEXT          NOT NULL,
                    payload     TEXT,
                    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
                )
                """);
        // UPDATE/DELETE 이벤트의 before 이미지에 전체 컬럼을 담기 위한 설정.
        // 기본값(DEFAULT)이면 before 에 PK 만 실려 필드 단위 변경 식별이 불가능하다.
        Db.onSource("ALTER TABLE verify_record REPLICA IDENTITY FULL");
        recreatePublication(RECORD_PUBLICATION, RECORD_TABLE);
    }

    protected static void recreatePublication(String publication, String table) {
        Db.onSource(
                "DROP PUBLICATION IF EXISTS " + publication,
                "CREATE PUBLICATION " + publication + " FOR TABLE " + table);
    }

    protected static void truncateRecords() {
        // TRUNCATE 는 op=t 라 캡처되지 않는다. 검증 데이터를 지울 때는 오히려 그 편이 낫다 —
        // 정리 작업이 이벤트로 흘러 들어와 다음 테스트를 오염시키지 않는다.
        Db.onSource("TRUNCATE TABLE verify_record RESTART IDENTITY");
    }

    // ── 복제 슬롯 관측 ─────────────────────────────────────────────────────

    protected static boolean slotExists(String slot) {
        Long n = Db.scalarOnSource(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = ?", Long.class, slot);
        return n != null && n > 0;
    }

    protected static void dropSlotQuietly(String slot) {
        if (!slotExists(slot)) {
            return;
        }
        try {
            Db.onSource("SELECT pg_drop_replication_slot('" + slot + "')");
        } catch (RuntimeException e) {
            // active 인 슬롯은 삭제되지 않는다. 엔진이 아직 붙어 있다는 뜻이므로 잠깐 뒤 재시도한다.
            sleep(1000);
            if (slotExists(slot)) {
                Db.onSource("SELECT pg_drop_replication_slot('" + slot + "')");
            }
        }
    }

    /** 슬롯이 붙잡고 있어 아직 지우지 못하는 WAL 크기(바이트). 다운타임 허용치 산출의 근거다. */
    protected static long slotRetainedWalBytes(String slot) {
        Long bytes = Db.scalarOnSource("""
                SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
                FROM pg_replication_slots WHERE slot_name = ?
                """, Long.class, slot);
        return bytes == null ? -1 : bytes;
    }

    protected static String slotConfirmedFlushLsn(String slot) {
        return Db.scalarOnSource(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = ?",
                String.class, slot);
    }

    protected static boolean slotActive(String slot) {
        Boolean active = Db.scalarOnSource(
                "SELECT active FROM pg_replication_slots WHERE slot_name = ?", Boolean.class, slot);
        return Boolean.TRUE.equals(active);
    }

    // ── 데이터 투입 ────────────────────────────────────────────────────────

    protected static void insertRecord(String bizKey, BigDecimal amount, String status) {
        Db.onSource("INSERT INTO verify_record (biz_key, amount, status) VALUES ("
                + "'" + bizKey + "', " + amount + ", '" + status + "')");
    }

    /** 한 트랜잭션으로 n 건을 넣는다. 대량 변경 처리량 측정용. */
    protected static void insertRecordsInOneTransaction(String prefix, int n) {
        Db.inSourceTransaction(connection -> {
            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO verify_record (biz_key, amount, status) VALUES (?, ?, ?)")) {
                for (int i = 1; i <= n; i++) {
                    ps.setString(1, prefix + "-" + i);
                    ps.setBigDecimal(2, BigDecimal.valueOf(1000L + i));
                    ps.setString(3, "NEW");
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (SQLException e) {
                throw new IllegalStateException("대량 INSERT 실패", e);
            }
        });
    }

    // ── 이벤트 판독 ────────────────────────────────────────────────────────

    /**
     * before 와 after 를 비교해 "실제로 바뀐 필드"만 뽑는다.
     * V1 의 필드 단위 변경 식별이 가능한지가 이 메서드가 값을 돌려주느냐로 판정된다.
     */
    protected static Set<String> changedFields(ChangeEvent event) {
        RowData before = event.before();
        RowData after = event.after();
        if (before == null || after == null) {
            return Set.of();
        }
        Set<String> changed = new TreeSet<>();
        Set<String> columns = new LinkedHashSet<>(before.values().keySet());
        columns.addAll(after.values().keySet());
        for (String column : columns) {
            Object b = before.values().get(column);
            Object a = after.values().get(column);
            if (b == null ? a != null : !b.equals(a)) {
                changed.add(column);
            }
        }
        return changed;
    }

    /** Debezium 이 값을 실을 수 없을 때 채워 넣는 자리표시자. V5 의 판정 기준이다. */
    protected static final String TOAST_PLACEHOLDER = "__debezium_unavailable_value";

    protected static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected static Connection openSource() throws SQLException {
        return Db.source();
    }
}
