package dev.embeddedcdc.verification;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V6-b. 대사의 사각지대와 구간 한정 전략.
 *
 * <p>{@link V6ReconciliationTest} 는 "유실을 주입하면 대사가 잡는다"까지 보였고,
 * DLQ 를 뺀 판정식과 고아 참조 지표는 exporter·경보에 이미 들어가 있다.
 * 남은 것은 <b>그 지표들이 실제로 그 상황을 잡는지</b>와, 상시로 돌릴 수 없는 체크섬 대사를
 * 어떻게 운영할지다.
 *
 * <p><b>1. 격리는 대사를 통과시킨다.</b>
 * 판정식 {@code |source − target| − DLQ(PENDING) = 0} 은 "격리로 설명되는 차이"를 정상으로 본다.
 * 옳은 판정이지만, 그 격리된 행이 <b>다른 행이 참조하는 부모</b>였다면 수신 측에는 고아가 남는다.
 * 건수 대사는 통과하고 체크섬 대사도 각 표에서는 통과하는데 관계만 끊어져 있는 상태다 —
 * 이것을 잡는 것은 고아 참조 대사뿐이다. 그 상황을 실제로 만들어 확인한다.
 *
 * <p><b>2. 체크섬 대사는 상시로 돌릴 수 없다.</b>
 * {@code md5(string_agg(...))} 는 정렬을 동반한 전체 스캔이다. 그래서 전체 대사(야간)와
 * 구간 대사(상시)를 나눈다. 나눈 대가가 무엇인지 — 구간 밖의 조용한 어긋남을 못 잡는다는 것 —
 * 까지 측정해야 전략이 성립한다.
 *
 * <p>대사 질의는 테스트가 따로 들고 있지 않고 {@code scripts/reconcile.sql} 을,
 * 고아 참조 질의는 exporter 의 {@code queries-target.yaml} 을 읽어 그대로 실행한다.
 * 검증에서 통과한 질의와 운영이 돌리는 질의가 다르면 검증한 적 없는 것과 같다.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("V6-b. 대사의 사각지대와 구간 한정")
class V6ReconciliationScopeTest extends VerificationSupport {

    private static final String ITEM = "V6-b";

    private static final Path RECONCILE_SQL = Path.of(
            System.getProperty("cdc.verify.reconcile.sql", "../../scripts/reconcile.sql"));
    private static final Path EXPORTER_YAML = Path.of(System.getProperty(
            "cdc.verify.exporter.target.yaml", "../../infra/monitoring/exporter/queries-target.yaml"));

    private static final String CONSTRAINT = "v6_orphan_chk";
    private static final String POISON_CODE = "V6-ORPHAN";
    private static final String ORPHAN_MEMBER = "V6-ORPHAN-MEMBER";
    private static final int GRADE_BATCH = 20;
    private static final int POISON_INDEX = 10;

    /** 비용 측정용 표의 크기. 구간이 전체의 2% 가 되도록 나눈다 — 운영에서 하루치가 차지하는 비율에 가깝다. */
    private static final String COST_TABLE = "verify_recon_cost";
    private static final int COST_ROWS = 50_000;
    private static final int RECENT_ROWS = 1_000;

    private static final Duration PROPAGATION_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration REPROCESS_TIMEOUT = Duration.ofSeconds(90);

    @BeforeAll
    static void prepare() {
        VerificationReport.section("V6-b. 대사의 사각지대와 구간 한정");
    }

    @AfterAll
    static void cleanup() {
        try {
            Db.onSource("DROP TABLE IF EXISTS " + COST_TABLE);
        } catch (RuntimeException e) {
            System.out.println("[" + ITEM + "] 정리 중 무시된 오류: " + e.getMessage());
        }
    }

    // ── 1. 대사 질의가 양쪽에서 성립하는가 ──────────────────────────────────

    @Test
    @Order(1)
    @DisplayName("어떤 표가 체크섬 대사까지 되고 어떤 표가 건수 대사만 되는가")
    void reconcileQueryTellsWhichTablesAreComparable() throws IOException {
        String sql = Files.readString(RECONCILE_SQL, StandardCharsets.UTF_8);
        Map<String, Recon> source = readReconcile(sql, true);
        Map<String, Recon> target = readReconcile(sql, false);

        assertThat(source.keySet()).as("양쪽에서 같은 표 집합이 나와야 한다").isEqualTo(target.keySet());

        int comparable = 0;
        for (String table : source.keySet()) {
            Recon s = source.get(table);
            Recon t = target.get(table);
            boolean sameShape = s.checksumColumns().equals(t.checksumColumns());
            boolean sameWindow = s.windowColumn().equals(t.windowColumn());
            boolean countsMatch = s.fullRows() == t.fullRows();

            VerificationReport.metric(ITEM, table + " · 건수",
                    s.fullRows() + " / " + t.fullRows() + (countsMatch ? " 일치" : " 불일치"));
            VerificationReport.metric(ITEM, table + " · 체크섬 대사",
                    sameShape ? (s.fullChecksum().equals(t.fullChecksum()) ? "가능 · 일치" : "가능 · 불일치")
                            : "불가 (양쪽 컬럼 집합이 다름 — 변환 테이블)");
            VerificationReport.metric(ITEM, table + " · 구간 대사",
                    sameWindow ? "가능 (" + s.windowColumn() + ")"
                            : "불가 (원천 " + s.windowColumn() + " vs 수신 " + t.windowColumn() + ")");

            assertThat(countsMatch).as(table + " 건수 대사").isTrue();
            if (sameShape) {
                comparable++;
                assertThat(s.fullChecksum())
                        .as(table + " 는 컬럼 집합이 같으므로 체크섬까지 일치해야 한다")
                        .isEqualTo(t.fullChecksum());
            }
        }

        assertThat(comparable).as("체크섬 대사가 되는 표가 하나는 있어야 시나리오가 성립한다").isPositive();
        VerificationReport.note(ITEM, "관리 컬럼(deleted, source_lsn, synced_at)을 뺀 컬럼 집합이 양쪽에서 같으면 "
                + "체크섬 대사가 성립한다. 변환이 걸린 표(computer)만 건수 대사로 제한되고, "
                + "그 표는 원천 시각을 수신이 보존하지 않아 구간 대사도 되지 않는다");
    }

    // ── 2. 격리는 대사를 통과시킨다 ─────────────────────────────────────────

    @Test
    @Order(2)
    @DisplayName("격리된 부모는 DLQ 판정식에 가려지고, 고아 참조 대사만 그것을 잡는다")
    void isolatedParentPassesReconciliationButLeavesOrphan() throws IOException {
        requireServiceRunning();
        cleanupScenarioRows();

        long dlqId;
        Db.onTarget("ALTER TABLE grade ADD CONSTRAINT " + CONSTRAINT
                + " CHECK (code <> '" + POISON_CODE + "')");
        try {
            insertGrades();
            dlqId = await("grade 격리", PROPAGATION_TIMEOUT, () -> Db.scalarOnTarget("""
                    SELECT id FROM cdc_dead_letter
                    WHERE pipeline = ? AND source_table = 'grade' AND status = 'PENDING'
                    ORDER BY id DESC LIMIT 1
                    """, Long.class, PIPELINE));
        } finally {
            Db.onTarget("ALTER TABLE grade DROP CONSTRAINT IF EXISTS " + CONSTRAINT);
        }

        // 격리된 등급을 참조하는 회원 — 원천에는 FK 가 있으므로 이 참조 자체는 정상이다
        long gradeId = Db.scalarOnSource(
                "SELECT id FROM grade WHERE code = ?", Long.class, POISON_CODE);
        Db.onSource("INSERT INTO member (email, name, grade_id, point) VALUES ('"
                + ORPHAN_MEMBER + "', 'V6', " + gradeId + ", 0)");
        await("회원 반영", PROPAGATION_TIMEOUT, () -> Db.scalarOnTarget(
                "SELECT id FROM member WHERE email = ?", Long.class, ORPHAN_MEMBER));

        // ── 대사는 무엇이라 말하는가 ──────────────────────────────────────
        long verdict = reconcileVerdict("grade");
        long orphans = orphanRows("member", "grade");

        VerificationReport.metric(ITEM, "격리 후 · grade 건수차 − DLQ(PENDING)", String.valueOf(verdict));
        VerificationReport.metric(ITEM, "격리 후 · 고아 참조(member→grade)", orphans + " 건");

        assertThat(verdict)
                .as("차이가 격리로 설명되므로 건수 대사는 통과한다 — 경보가 뜨지 않는다")
                .isZero();
        assertThat(orphans)
                .as("그런데 참조는 끊어져 있다. 이것을 잡는 것은 고아 참조 대사뿐이다")
                .isPositive();

        VerificationReport.note(ITEM, "DLQ 를 판정식에서 빼는 것은 옳지만, 그것만으로는 "
                + "'격리된 행이 다른 행의 부모였다'를 알 수 없다. 건수 대사가 통과하는 동안 하류는 이미 깨져 있다");

        // ── 재처리하면 함께 풀린다 ────────────────────────────────────────
        Db.updateOnTarget("UPDATE cdc_dead_letter SET status = 'RETRY_REQUESTED' WHERE id = " + dlqId);
        await("DLQ 재처리", REPROCESS_TIMEOUT, () -> {
            String status = Db.scalarOnTarget(
                    "SELECT status FROM cdc_dead_letter WHERE id = ?", String.class, dlqId);
            return "RETRY_REQUESTED".equals(status) ? null : status;
        });
        long orphansAfter = await("고아 해소", PROPAGATION_TIMEOUT,
                () -> orphanRows("member", "grade") == 0 ? 0L : null);

        VerificationReport.metric(ITEM, "재처리 후 · grade 건수차 − DLQ(PENDING)",
                String.valueOf(reconcileVerdict("grade")));
        VerificationReport.metric(ITEM, "재처리 후 · 고아 참조", orphansAfter + " 건");
        VerificationReport.note(ITEM, "부모가 반영되면 고아도 함께 사라진다 — "
                + "고아 대사는 '지금 조치가 필요한가'를 말해 주는 지표이지 영구 손상의 표시가 아니다");

        cleanupScenarioRows();
    }

    // ── 3. 전체 대사와 구간 대사의 비용 ─────────────────────────────────────

    @Test
    @Order(3)
    @DisplayName("전체 대사는 상시로 못 돌린다 — 구간 대사의 비용과 전제")
    void windowedReconciliationIsCheaperOnlyWithAnIndex() {
        createCostFixture();

        long fullRows = checksum(null).rows();
        long windowRows = checksum("24 hours").rows();
        VerificationReport.metric(ITEM, "비용 측정 표 크기", String.format(Locale.ROOT, "%,d 행", fullRows));
        VerificationReport.metric(ITEM, "그중 최근 24시간",
                String.format(Locale.ROOT, "%,d 행 (%.1f%%)", windowRows, 100.0 * windowRows / fullRows));

        long fullMs = timed(() -> checksum(null));
        long windowNoIndexMs = timed(() -> checksum("24 hours"));
        String planNoIndex = planOf("24 hours");

        Db.onSource("CREATE INDEX IF NOT EXISTS " + COST_TABLE + "_updated_at_idx ON "
                + COST_TABLE + " (updated_at)");
        Db.onSource("ANALYZE " + COST_TABLE);

        long windowIndexedMs = timed(() -> checksum("24 hours"));
        String planIndexed = planOf("24 hours");

        VerificationReport.metric(ITEM, "전체 대사 소요", fullMs + " ms");
        VerificationReport.metric(ITEM, "구간 대사 소요 · 인덱스 없음",
                windowNoIndexMs + " ms  (" + planNoIndex + ")");
        VerificationReport.metric(ITEM, "구간 대사 소요 · 인덱스 있음",
                windowIndexedMs + " ms  (" + planIndexed + ")");

        assertThat(windowRows).as("구간은 전체보다 작아야 한다").isLessThan(fullRows);
        assertThat(windowNoIndexMs).as("구간을 좁히는 것만으로도 전체 대사보다 싸야 한다").isLessThan(fullMs);
        assertThat(planNoIndex).as("인덱스가 없으면 조건만 붙은 전체 스캔이다").contains("Seq Scan");
        assertThat(planIndexed).as("인덱스가 있으면 구간만 읽는다").doesNotContain("Seq Scan");

        VerificationReport.note(ITEM, "이 규모에서 비용을 만드는 것은 스캔이 아니라 정렬·집계다 — "
                + "구간을 2% 로 좁히는 것만으로 " + fullMs + "ms 가 " + windowNoIndexMs + "ms 로 줄었고, "
                + "인덱스는 읽는 방식만 바꿨을 뿐 시간은 " + windowIndexedMs + "ms 로 같았다. "
                + "인덱스가 값을 하는 것은 표가 커져 스캔이 지배적이 될 때이며, 선택도가 낮으면 "
                + "플래너가 아예 쓰지 않으므로 구간 폭은 표 크기에 맞춰 정해야 한다");
    }

    // ── 4. 구간 대사가 못 잡는 것 ───────────────────────────────────────────

    @Test
    @Order(4)
    @DisplayName("구간 밖에서 조용히 어긋난 행은 구간 대사가 못 잡는다")
    void windowedReconciliationMissesOldRows() {
        Checksum fullBefore = checksum(null);
        Checksum windowBefore = checksum("24 hours");

        // 구간 밖 행의 값만 바꾼다. updated_at 은 건드리지 않는다 —
        // 수신 측에서 값이 조용히 어긋나는 상황이 정확히 이 모양이다.
        Db.onSource("UPDATE " + COST_TABLE + " SET amount = amount + 1 WHERE id = "
                + "(SELECT min(id) FROM " + COST_TABLE
                + " WHERE updated_at < now() - interval '24 hours')");

        Checksum fullAfter = checksum(null);
        Checksum windowAfter = checksum("24 hours");

        VerificationReport.metric(ITEM, "구간 밖 1행 변조 · 전체 대사",
                fullBefore.value().equals(fullAfter.value()) ? "검출 못 함" : "검출");
        VerificationReport.metric(ITEM, "구간 밖 1행 변조 · 구간 대사",
                windowBefore.value().equals(windowAfter.value()) ? "검출 못 함" : "검출");

        assertThat(fullAfter.value()).as("전체 대사는 잡는다").isNotEqualTo(fullBefore.value());
        assertThat(windowAfter.value()).as("구간 대사는 못 잡는다 — 이것이 나눈 대가다")
                .isEqualTo(windowBefore.value());

        VerificationReport.note(ITEM, "구간 대사와 전체 대사는 대체재가 아니라 짝이다 — "
                + "상시(수십 분 주기)로 구간을 돌려 빨리 잡고, 야간 배치로 전체를 한 번 훑는다. "
                + "구간만 돌리면 오래된 행의 어긋남이 영원히 안 잡힌다");
    }

    // ── 대사 질의 실행 ─────────────────────────────────────────────────────

    private record Recon(String table, String checksumColumns, String windowColumn,
                         long fullRows, String fullChecksum,
                         Long windowRows, String windowChecksum) {
    }

    private static Map<String, Recon> readReconcile(String sql, boolean onSource) {
        List<Object[]> rows = onSource ? Db.rowsOnSource(sql) : Db.rowsOnTarget(sql);
        Map<String, Recon> out = new LinkedHashMap<>();
        for (Object[] r : rows) {
            out.put(String.valueOf(r[0]), new Recon(
                    String.valueOf(r[0]),
                    String.valueOf(r[1]),
                    String.valueOf(r[2]),
                    ((Number) r[3]).longValue(),
                    String.valueOf(r[4]),
                    r[5] == null ? null : ((Number) r[5]).longValue(),
                    r[6] == null ? null : String.valueOf(r[6])));
        }
        return out;
    }

    /** 운영 경보와 같은 판정식: |source − target| − DLQ(PENDING). 0 이면 대사 통과다. */
    private static long reconcileVerdict(String table) {
        long src = Db.scalarOnSource("SELECT count(*) FROM " + table, Long.class);
        long tgt = Db.scalarOnTarget("SELECT count(*) FROM " + table + " WHERE deleted = false", Long.class);
        long pending = Db.scalarOnTarget("""
                SELECT count(*) FROM cdc_dead_letter
                WHERE source_table = ? AND status = 'PENDING'
                """, Long.class, table);
        return Math.abs(src - tgt) - pending;
    }

    /** exporter 가 대시보드에 내는 것과 같은 질의로 고아 행을 센다. */
    private static long orphanRows(String child, String parent) {
        try {
            String yaml = Files.readString(EXPORTER_YAML, StandardCharsets.UTF_8);
            Map<String, Object> root = new Yaml().load(yaml);
            Map<?, ?> orphan = (Map<?, ?>) root.get("cdc_orphan");
            String sql = String.valueOf(orphan.get("query"));

            for (Object[] r : Db.rowsOnTarget(sql)) {
                if (child.equals(String.valueOf(r[0])) && parent.equals(String.valueOf(r[1]))) {
                    return ((Number) r[2]).longValue();
                }
            }
            return 0;
        } catch (IOException e) {
            throw new IllegalStateException("exporter 질의를 읽지 못했다: " + EXPORTER_YAML, e);
        }
    }

    // ── 비용 측정 ──────────────────────────────────────────────────────────

    private record Checksum(long rows, String value) {
    }

    /** reconcile.sql 과 같은 모양의 체크섬. window 가 null 이면 전체다. */
    private static Checksum checksum(String window) {
        String where = window == null ? "" : " WHERE t.updated_at >= now() - interval '" + window + "'";
        Object[] row = Db.rowsOnSource("""
                SELECT count(*)::bigint AS c,
                       coalesce(md5(string_agg(
                           (to_jsonb(t) - 'deleted' - 'source_lsn' - 'synced_at')::text,
                           ',' ORDER BY t.id)), '') AS k
                FROM %s t%s
                """.formatted(COST_TABLE, where)).get(0);
        return new Checksum(((Number) row[0]).longValue(), String.valueOf(row[1]));
    }

    /** 한 번 돌려 캐시를 데운 뒤 잰다. 첫 회차의 디스크 읽기가 비교를 흐린다. */
    private static long timed(Runnable work) {
        work.run();
        long start = System.nanoTime();
        work.run();
        return (System.nanoTime() - start) / 1_000_000;
    }

    /** 구간 질의가 실제로 어떤 방식으로 읽는지. 시간보다 이쪽이 환경에 덜 흔들린다. */
    private static String planOf(String window) {
        List<Object[]> rows = Db.rowsOnSource(
                "EXPLAIN SELECT count(*) FROM " + COST_TABLE
                        + " t WHERE t.updated_at >= now() - interval '" + window + "'");
        // 읽는 방식은 두 번째 줄(스캔 노드)에 나온다. 전체 계획을 리포트에 넣으면 읽히지 않는다.
        StringBuilder plan = new StringBuilder();
        for (int i = 0; i < Math.min(2, rows.size()); i++) {
            if (i > 0) {
                plan.append(" / ");
            }
            plan.append(String.valueOf(rows.get(i)[0]).trim().replaceAll("\\s+\\(cost.*", ""));
        }
        return plan.toString();
    }

    private static void createCostFixture() {
        Db.onSource("DROP TABLE IF EXISTS " + COST_TABLE);
        Db.onSource("""
                CREATE TABLE %s (
                    id         BIGSERIAL     PRIMARY KEY,
                    biz_key    TEXT          NOT NULL,
                    amount     NUMERIC(14,2) NOT NULL,
                    status     TEXT          NOT NULL,
                    updated_at TIMESTAMPTZ   NOT NULL
                )
                """.formatted(COST_TABLE));

        Db.inSourceTransaction(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + COST_TABLE + " (biz_key, amount, status, updated_at) "
                            + "VALUES (?, ?, ?, now() - (? * interval '1 day'))")) {
                for (int i = 1; i <= COST_ROWS; i++) {
                    ps.setString(1, "V6B-" + i);
                    ps.setBigDecimal(2, BigDecimal.valueOf(1000L + i));
                    ps.setString(3, "NEW");
                    // 최근 구간에 들어갈 행은 앞쪽 RECENT_ROWS 건만. 나머지는 이틀 전이다.
                    ps.setInt(4, i <= RECENT_ROWS ? 0 : 2);
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (SQLException e) {
                throw new IllegalStateException("비용 측정 표 적재 실패", e);
            }
        });
        Db.onSource("ANALYZE " + COST_TABLE);
    }

    // ── 시나리오 데이터 ────────────────────────────────────────────────────

    private static void insertGrades() {
        Db.inSourceTransaction(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO grade (code, name, discount_rate) VALUES (?, 'V6', ?)")) {
                for (int i = 1; i <= GRADE_BATCH; i++) {
                    ps.setString(1, i == POISON_INDEX ? POISON_CODE : "V6-grade-" + i);
                    ps.setBigDecimal(2, BigDecimal.valueOf(i % 10));
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (SQLException e) {
                throw new IllegalStateException("grade 적재 실패", e);
            }
        });
    }

    /** 원천에서 지우고 전파를 기다린 뒤, 소프트 삭제로 남은 행을 물리 삭제한다. DLQ 는 증거라 남긴다. */
    private static void cleanupScenarioRows() {
        try {
            Db.onSource("DELETE FROM member WHERE email LIKE 'V6-%'");
            Db.onSource("DELETE FROM grade WHERE code LIKE 'V6-%'");
            sleep(3000);
            Db.onTarget("DELETE FROM member WHERE email LIKE 'V6-%'");
            Db.onTarget("DELETE FROM grade WHERE code LIKE 'V6-%'");
        } catch (RuntimeException e) {
            System.out.println("[" + ITEM + "] 정리 중 무시된 오류: " + e.getMessage());
        }
    }
}
