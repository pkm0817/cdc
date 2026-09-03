package dev.embeddedcdc.verification;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V4-b. DLQ 재처리 경로의 중복 유입 — 테이블별 가드.
 *
 * <p>{@link V4DuplicateDeliveryTest} 는 "오프셋이 뒤처진 채 재기동되는" 경로 하나만 본다.
 * 중복이 유입되는 경로는 그것 말고 하나 더 있다 — <b>DLQ 재처리</b>다.
 * 격리된 건을 {@code status = 'RETRY_REQUESTED'} 로 표시하면 재처리기가 그 이벤트를 다시 적용하는데,
 * 격리된 뒤 재처리까지의 사이에 정상 경로로 같은 행이 이미 갱신됐을 수 있다.
 * 그러면 재처리는 "중복"이 아니라 "역전"이다 — 오래된 값이 최신 값을 덮어쓸 수 있다.
 *
 * <p><b>그리고 그것을 막는 기제가 테이블마다 다르다.</b>
 * computer / grade / member 는 {@code WHERE EXCLUDED.source_lsn > ...} 가드가 붙은 UPSERT 라
 * 오래된 LSN 이 차단되지만, car 는 조건 없는 merge 라 막을 것이 없다.
 * 한 테이블만 보고 V4 를 통과시키면 이 차이가 보고서에서 사라진다.
 *
 * <p><b>이 항목만은 검증 전용 테이블을 쓰지 않는다.</b> 다른 시나리오는 verify_* 테이블과
 * 전용 슬롯으로 돌지만, 여기서 검증하려는 것이 곧 각 테이블의 저장소 구현과 운영 DLQ 재처리기다.
 * 복제본을 만들면 정작 운영에서 쓰는 경로가 검증되지 않는다. 그래서 기동 중인 emb-cdc-service 와
 * 운영 테이블을 그대로 쓰고, 테스트가 넣은 행은 끝에서 원천·수신 양쪽에서 지운다.
 *
 * <p>한 테이블당 흐름:
 * <pre>
 *   1. 수신 테이블에 CHECK 제약을 걸어 특정 행 하나만 적재를 실패시킨다 (격리 유도)
 *   2. 20건을 한 트랜잭션으로 넣는다 → 19건 반영, 1건 DLQ PENDING
 *   3. 제약을 없앤다 (= 원인 수정)
 *   4. 그 사이 정상 경로로 같은 행이 갱신된다 → 수신측에 더 새로운 LSN 이 남는다
 *   5. RETRY_REQUESTED 로 표시 → 재처리기가 오래된 이벤트를 다시 적용한다
 *   6. 최신 값이 살아남았는지 본다
 * </pre>
 */
@DisplayName("V4-b. DLQ 재처리 중복 유입과 테이블별 가드")
class V4ReprocessGuardTest extends VerificationSupport {

    private static final String ITEM = "V4-b";

    /**
     * 한 배치에 넣는 건수. 격리 비율이 halt-on-dead-letter-ratio(0.5)를 넘으면 파이프라인이
     * 멈추므로, 실패 1건이 배치에 혼자 남지 않도록 넉넉히 넣고 가운데를 오염시킨다.
     */
    private static final int BATCH = 20;
    private static final int POISON_INDEX = 10;

    private static final String POISON = "V4-POISON";
    private static final String FIXED = "V4-FIXED";
    private static final String CONSTRAINT = "v4_reprocess_guard_chk";

    /** 재처리기 주기는 30초다(cdc.dead-letter.reprocess-interval-ms). 그 두 배 남짓 기다린다. */
    private static final Duration REPROCESS_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration PROPAGATION_TIMEOUT = Duration.ofSeconds(60);

    @BeforeAll
    static void prepare() {
        VerificationReport.section("V4-b. DLQ 재처리 경로의 중복 유입 — 테이블별 가드");
        requireServiceRunning();
        VerificationReport.note(ITEM,
                "이 항목은 verify_* 전용 테이블이 아니라 운영 테이블과 기동 중인 emb-cdc-service 를 그대로 쓴다 — "
                        + "검증 대상이 각 테이블의 저장소 구현과 운영 DLQ 재처리기 자체라서 복제본으로는 답이 나오지 않는다");
    }

    @AfterAll
    static void report() {
        VerificationReport.note(ITEM,
                "DLQ 는 재처리 결과를 예외 유무로만 판정한다 — 가드에 막혀 0행이 반영돼도 RESOLVED 가 된다. "
                        + "상태만으로는 '반영됨'과 '차단됨'을 구분할 수 없으므로, 재처리 결과에 반영 행 수를 남길 것");
    }

    // ── 테이블별 시나리오 ──────────────────────────────────────────────────

    @Test
    @DisplayName("computer — LSN 가드가 재처리의 역전을 막는다")
    void computerGuardBlocksStaleReprocess() {
        runCase(new Fixture("computer", true,
                "full_name <> 'V4BRAND V4-POISON'",
                "model", "full_name",
                "V4BRAND " + POISON, "V4BRAND " + FIXED));
    }

    @Test
    @DisplayName("grade — LSN 가드가 재처리의 역전을 막는다")
    void gradeGuardBlocksStaleReprocess() {
        runCase(new Fixture("grade", true,
                "code <> '" + POISON + "'",
                "code", "code",
                POISON, FIXED));
    }

    @Test
    @DisplayName("member — LSN 가드가 재처리의 역전을 막는다")
    void memberGuardBlocksStaleReprocess() {
        runCase(new Fixture("member", true,
                "email <> '" + POISON + "'",
                "email", "email",
                POISON, FIXED));
    }

    @Test
    @DisplayName("car — 가드가 없어 재처리가 최신 값을 덮어쓴다")
    void carHasNoGuardAndRegresses() {
        runCase(new Fixture("car", false,
                "name <> '" + POISON + "'",
                "name", "name",
                POISON, FIXED));
    }

    // ── 시나리오 본체 ──────────────────────────────────────────────────────

    /**
     * @param table            원천 테이블 이름
     * @param guarded          수신 저장소에 source_lsn 가드가 있는지. 판정의 기대값이 갈린다
     * @param targetCheckExpr  적재를 실패시킬 CHECK 식 (수신 테이블 기준)
     * @param sourceKeyColumn  원천에서 그 행을 찾는 컬럼
     * @param targetKeyColumn  수신에서 값을 읽을 컬럼
     * @param poisonTargetValue 격리된 이벤트가 다시 반영되면 수신에 나타날 값
     * @param fixedTargetValue  정상 경로로 갱신된 뒤 수신에 있어야 할 값
     */
    private record Fixture(String table, boolean guarded, String targetCheckExpr,
                           String sourceKeyColumn, String targetKeyColumn,
                           String poisonTargetValue, String fixedTargetValue) {
    }

    private void runCase(Fixture f) {
        cleanupQuietly(f);
        long dlqId;
        long poisonRowId;

        // ── 1. 적재를 실패시킬 원인을 만든다 ────────────────────────────────
        Db.onTarget("ALTER TABLE " + f.table() + " ADD CONSTRAINT " + CONSTRAINT
                + " CHECK (" + f.targetCheckExpr() + ")");
        try {
            // ── 2. 20건 중 1건만 실패한다 ───────────────────────────────────
            insertBatch(f);
            poisonRowId = awaitPoisonRowId(f);

            dlqId = await("DLQ 격리", PROPAGATION_TIMEOUT, () -> Db.scalarOnTarget("""
                    SELECT id FROM cdc_dead_letter
                    WHERE pipeline = ? AND source_table = ? AND status = 'PENDING'
                    ORDER BY id DESC LIMIT 1
                    """, Long.class, PIPELINE, f.table()));
        } finally {
            // ── 3. 원인 수정 ───────────────────────────────────────────────
            Db.onTarget("ALTER TABLE " + f.table() + " DROP CONSTRAINT IF EXISTS " + CONSTRAINT);
        }

        long dlqLsn = Db.scalarOnTarget(
                "SELECT source_lsn FROM cdc_dead_letter WHERE id = ?", Long.class, dlqId);
        long appliedRows = countApplied(f, poisonRowId);

        VerificationReport.metric(ITEM, f.table() + " · 배치 건수", BATCH + "건 중 1건 격리");
        VerificationReport.metric(ITEM, f.table() + " · 격리 외 반영", appliedRows + "건");
        VerificationReport.metric(ITEM, f.table() + " · 격리된 이벤트 LSN", String.valueOf(dlqLsn));

        assertThat(appliedRows)
                .as("격리된 1건을 뺀 나머지는 그대로 반영돼야 한다")
                .isEqualTo(BATCH - 1L);

        // ── 4. 재처리 전에 정상 경로로 같은 행이 갱신된다 ────────────────────
        Db.onSource(fixSql(f.table(), poisonRowId));
        await("정상 경로 반영", PROPAGATION_TIMEOUT,
                () -> f.fixedTargetValue().equals(targetValue(f, poisonRowId)) ? Boolean.TRUE : null);

        Long lsnBefore = guardColumnLsn(f, poisonRowId);
        VerificationReport.metric(ITEM, f.table() + " · 재처리 전 수신 값", targetValue(f, poisonRowId));
        VerificationReport.metric(ITEM, f.table() + " · 재처리 전 수신 LSN",
                lsnBefore == null ? "(가드 컬럼 없음)" : String.valueOf(lsnBefore));

        assertThat(dlqLsn)
                .as("격리된 이벤트는 정상 경로 갱신보다 오래된 것이어야 시나리오가 성립한다")
                .isLessThan(lsnBefore == null ? Long.MAX_VALUE : lsnBefore);

        // ── 5. 재처리 신청 ─────────────────────────────────────────────────
        Db.updateOnTarget("UPDATE cdc_dead_letter SET status = 'RETRY_REQUESTED' WHERE id = " + dlqId);
        String finalStatus = await("DLQ 재처리", REPROCESS_TIMEOUT, () -> {
            String status = Db.scalarOnTarget(
                    "SELECT status FROM cdc_dead_letter WHERE id = ?", String.class, dlqId);
            return "RETRY_REQUESTED".equals(status) ? null : status;
        });

        // ── 6. 판정 ────────────────────────────────────────────────────────
        String valueAfter = targetValue(f, poisonRowId);
        Long lsnAfter = guardColumnLsn(f, poisonRowId);

        VerificationReport.metric(ITEM, f.table() + " · 재처리 후 DLQ 상태", finalStatus);
        VerificationReport.metric(ITEM, f.table() + " · 재처리 후 수신 값", valueAfter);
        VerificationReport.metric(ITEM, f.table() + " · 재처리 후 수신 LSN",
                lsnAfter == null ? "(가드 컬럼 없음)" : String.valueOf(lsnAfter));

        assertThat(finalStatus).as("재처리는 예외 없이 끝나야 한다").isEqualTo("RESOLVED");

        if (f.guarded()) {
            VerificationReport.note(ITEM, f.table()
                    + ": 오래된 이벤트가 LSN 가드에 막혀 0행 반영 — 최신 값이 그대로 남았다");
            assertThat(valueAfter)
                    .as("가드가 있는 테이블은 오래된 재처리가 최신 값을 덮어쓰지 않는다")
                    .isEqualTo(f.fixedTargetValue());
            assertThat(lsnAfter).as("가드가 막았으므로 수신 LSN 도 그대로다").isEqualTo(lsnBefore);
        } else {
            VerificationReport.note(ITEM, f.table()
                    + ": 조건 없는 merge 라 재처리가 최신 값을 격리 시점 값으로 되돌렸다 — "
                    + "DLQ 재처리는 이 테이블에서 안전하지 않다");
            assertThat(valueAfter)
                    .as("가드가 없는 테이블은 오래된 재처리가 최신 값을 덮어쓴다 (현 구현의 사실)")
                    .isEqualTo(f.poisonTargetValue());
        }

        cleanupQuietly(f, poisonRowId);
    }

    // ── 원천 조작 ──────────────────────────────────────────────────────────

    private static void insertBatch(Fixture f) {
        Db.inSourceTransaction(connection -> {
            try (PreparedStatement ps = connection.prepareStatement(insertSql(f.table()))) {
                for (int i = 1; i <= BATCH; i++) {
                    String key = (i == POISON_INDEX) ? POISON : "V4-" + f.table() + "-" + i;
                    bindInsert(ps, f.table(), key, i);
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (SQLException e) {
                throw new IllegalStateException(f.table() + " 대량 INSERT 실패", e);
            }
        });
    }

    private static String insertSql(String table) {
        return switch (table) {
            case "car" -> "INSERT INTO car (name, brand, price) VALUES (?, 'V4BRAND', ?)";
            case "computer" -> "INSERT INTO computer (brand, model, cpu, ram_gb, price_usd) "
                    + "VALUES ('V4BRAND', ?, 'V4CPU', 8, ?)";
            case "grade" -> "INSERT INTO grade (code, name, discount_rate) VALUES (?, 'V4', ?)";
            // grade_id 는 시드 등급을 그대로 쓴다. 이 시나리오가 보는 것은 참조가 아니라 가드다.
            case "member" -> "INSERT INTO member (email, name, grade_id, point) "
                    + "VALUES (?, 'V4', (SELECT min(id) FROM grade), ?)";
            default -> throw new IllegalArgumentException("모르는 테이블: " + table);
        };
    }

    private static void bindInsert(PreparedStatement ps, String table, String key, int i)
            throws SQLException {
        ps.setString(1, key);
        switch (table) {
            case "car" -> ps.setBigDecimal(2, BigDecimal.valueOf(1000L + i));
            case "computer" -> ps.setBigDecimal(2, BigDecimal.valueOf(100L + i));
            case "grade" -> ps.setBigDecimal(2, BigDecimal.valueOf(i % 10));
            case "member" -> ps.setInt(2, i);
            default -> throw new IllegalArgumentException("모르는 테이블: " + table);
        }
    }

    /** 격리된 그 행을 정상 경로로 갱신한다 — 재처리보다 새로운 LSN 을 수신측에 남기는 단계다. */
    private static String fixSql(String table, long id) {
        return switch (table) {
            case "car" -> "UPDATE car SET name = '" + FIXED + "', updated_at = now() WHERE id = " + id;
            case "computer" -> "UPDATE computer SET model = '" + FIXED + "' WHERE id = " + id;
            case "grade" -> "UPDATE grade SET code = '" + FIXED + "' WHERE id = " + id;
            case "member" -> "UPDATE member SET email = '" + FIXED + "', updated_at = now() WHERE id = " + id;
            default -> throw new IllegalArgumentException("모르는 테이블: " + table);
        };
    }

    private static long awaitPoisonRowId(Fixture f) {
        return await("원천 오염 행", PROPAGATION_TIMEOUT, () -> Db.scalarOnSource(
                "SELECT id FROM " + f.table() + " WHERE " + f.sourceKeyColumn() + " = ?",
                Long.class, POISON));
    }

    // ── 수신 관측 ──────────────────────────────────────────────────────────

    private static String targetValue(Fixture f, long id) {
        return Db.scalarOnTarget(
                "SELECT " + f.targetKeyColumn() + " FROM " + f.table() + " WHERE id = ?", String.class, id);
    }

    /** 가드 컬럼이 있는 테이블만 값을 준다. car 는 컬럼 자체가 없어 null 이다. */
    private static Long guardColumnLsn(Fixture f, long id) {
        if (!f.guarded()) {
            return null;
        }
        return Db.scalarOnTarget(
                "SELECT source_lsn FROM " + f.table() + " WHERE id = ?", Long.class, id);
    }

    /** 격리된 1건을 뺀 나머지가 수신에 도달했는지 센다. */
    private static long countApplied(Fixture f, long poisonRowId) {
        return await("나머지 반영", PROPAGATION_TIMEOUT, () -> {
            Long n = Db.scalarOnTarget("SELECT count(*) FROM " + f.table()
                    + " WHERE " + f.targetKeyColumn() + " LIKE 'V4%' AND id <> ?", Long.class, poisonRowId);
            return n != null && n >= BATCH - 1L ? n : null;
        });
    }

    // ── 정리 ───────────────────────────────────────────────────────────────

    private static void cleanupQuietly(Fixture f) {
        cleanupQuietly(f, null);
    }

    /**
     * 원천에서 지우고 그 DELETE 가 수신에 닿을 때까지 기다린 뒤, 소프트 삭제로 남은 행을 물리 삭제한다.
     * 운영 테이블을 쓰는 시나리오이므로 흔적을 남기지 않는다.
     *
     * 감사 로그도 지운다 — 정상 경로 갱신(4단계)이 감사 대상 테이블(car)에서는 cdc_change_audit 에
     * 한 줄을 남기는데, 그것까지 지우지 않으면 이미 사라진 행을 가리키는 이력이 매 회차 쌓인다.
     * DLQ 행만은 남긴다. 그쪽은 측정의 증거다.
     */
    private static void cleanupQuietly(Fixture f, Long rowId) {
        try {
            String where = f.sourceKeyColumn() + " LIKE 'V4%'";
            Db.onSource("DELETE FROM " + f.table() + " WHERE " + where);
            sleep(3000);
            Db.onTarget("DELETE FROM " + f.table() + " WHERE " + f.targetKeyColumn() + " LIKE 'V4%'");
            if (rowId != null) {
                Db.onTarget("DELETE FROM cdc_change_audit WHERE source_table = '" + f.table()
                        + "' AND row_key = '" + rowId + "'");
            }
        } catch (RuntimeException e) {
            System.out.println("[" + ITEM + "] 정리 중 무시된 오류: " + e.getMessage());
        }
    }

}
