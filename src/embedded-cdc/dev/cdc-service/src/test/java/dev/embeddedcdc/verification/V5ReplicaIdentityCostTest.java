package dev.embeddedcdc.verification;

import dev.embeddedcdc.domain.model.ChangeEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V5-b. 관심 필드 판별 방법과 REPLICA IDENTITY FULL 의 대가.
 *
 * <p>{@link V5ToastedFieldTest} 는 "DEFAULT 면 못 읽고 FULL 이면 읽힌다"까지 보였다.
 * 남은 것은 그 결론을 실제로 쓸 수 있게 만드는 두 가지다.
 *
 * <p><b>1. 관심 필드가 이 케이스에 걸리는지 판별하는 방법.</b>
 * 방법이 정해져 있지 않으면 "우리 테이블은 괜찮은가"에 답할 수 없어 결론이 실행되지 않는다.
 * 여기서는 2단계 절차를 확정한다 — 정적 선별(SQL) 뒤 동적 확정(이벤트 관측).
 * 1단계 질의는 테스트가 따로 들고 있지 않고 {@code scripts/toast-candidates.sql} 을 그대로 읽어 실행한다.
 * 검증에서 통과한 질의와 운영자가 손으로 돌리는 질의가 다르면 검증한 적 없는 것과 같다.
 *
 * <p><b>2. FULL 로 바꿨을 때의 대가.</b>
 * FULL 은 UPDATE/DELETE 마다 변경 전 행 전체를 WAL 에 쓴다. 대용량 컬럼이 있으면
 * 그 값을 <b>TOAST 에서 꺼내서(detoast)</b> 통째로 WAL 에 넣는다. 그래서 V5 의 요구가
 * 그대로 V2 의 디스크 압력이 된다. 같은 부하로 DEFAULT 와 FULL 을 나란히 재서 그 폭을 수치로 만든다.
 *
 * <p>측정 방법: 각 구간 직전에 {@code CHECKPOINT} 를 걸어 full page image 조건을 맞추고,
 * {@code pg_current_wal_lsn()} 차분으로 총량을, {@code pg_stat_wal} 로 레코드 수와 FPI 수를 함께 남긴다.
 * WAL 은 클러스터 전체 공용이라 유휴 잡음이 섞이므로, 같은 대기 시간의 유휴 바닥값을 먼저 재서 함께 기록한다.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("V5-b. TOAST 후보 판별과 REPLICA IDENTITY 비용")
class V5ReplicaIdentityCostTest extends VerificationSupport {

    private static final String ITEM = "V5-b";

    /** 1단계 선별 질의. 운영자가 손으로 돌리는 것과 같은 파일을 읽는다. */
    private static final Path CANDIDATE_SQL = Path.of(
            System.getProperty("cdc.verify.toast.sql", "../../scripts/toast-candidates.sql"));

    private static final String PROBE_TABLE = "verify_toast_probe";
    private static final String PROBE_PUBLICATION = "verify_toast_probe_pub";
    private static final String PROBE_SLOT = "verify_v5b_slot";

    private static final String NARROW = "verify_wal_narrow";
    private static final String WIDE = "verify_wal_wide";

    /** TOAST_TUPLE_THRESHOLD. 블록 8KB 의 약 1/4 — 질의의 판정 기준과 같은 값이어야 한다. */
    private static final long TOAST_THRESHOLD = 2032;

    /** 운영 파이프라인을 거쳐 수신측에 닿기를 기다리는 한도. */
    private static final Duration PROPAGATION_TIMEOUT = Duration.ofSeconds(60);

    private static final int PAYLOAD_SIZE = 200_000;
    private static final int NARROW_ROWS = 5_000;
    private static final int WIDE_ROWS = 100;

    @BeforeAll
    static void prepare() {
        VerificationReport.section("V5-b. TOAST 후보 판별과 REPLICA IDENTITY 비용");

        // 관심 필드 하나(big_blob)와 그렇지 않은 필드 하나(small_note)를 같이 둔다 —
        // 규칙이 "무엇을 고르는가"뿐 아니라 "무엇을 고르지 않는가"까지 맞아야 쓸 수 있다.
        Db.onSource("""
                CREATE TABLE IF NOT EXISTS verify_toast_probe (
                    id         BIGSERIAL PRIMARY KEY,
                    small_note TEXT      NOT NULL,
                    big_blob   TEXT      NOT NULL
                )
                """);
        // 압축되어 인라인에 남으면 TOAST 상황이 재현되지 않는다. EXTERNAL 은 압축 없이 행 밖으로 보낸다.
        Db.onSource("ALTER TABLE verify_toast_probe ALTER COLUMN big_blob SET STORAGE EXTERNAL");
        Db.onSource("ALTER TABLE verify_toast_probe REPLICA IDENTITY DEFAULT");

        Db.onSource("""
                CREATE TABLE IF NOT EXISTS verify_wal_narrow (
                    id         BIGSERIAL     PRIMARY KEY,
                    code       TEXT          NOT NULL,
                    amount     NUMERIC(14,2) NOT NULL,
                    status     TEXT          NOT NULL,
                    updated_at TIMESTAMPTZ   NOT NULL DEFAULT now()
                )
                """);
        Db.onSource("""
                CREATE TABLE IF NOT EXISTS verify_wal_wide (
                    id       BIGSERIAL PRIMARY KEY,
                    code     TEXT      NOT NULL,
                    big_blob TEXT      NOT NULL
                )
                """);
        Db.onSource("ALTER TABLE verify_wal_wide ALTER COLUMN big_blob SET STORAGE EXTERNAL");

        // 자동 청소가 측정 구간에 끼어들면 그 WAL 이 비용으로 잡힌다. 측정 대상만 남긴다.
        Db.onSource("ALTER TABLE verify_wal_narrow SET (autovacuum_enabled = false)");
        Db.onSource("ALTER TABLE verify_wal_wide SET (autovacuum_enabled = false)");
        Db.onSource("ALTER TABLE verify_toast_probe SET (autovacuum_enabled = false)");
    }

    @AfterAll
    static void cleanup() {
        dropSlotQuietly(PROBE_SLOT);
        try {
            Db.onSource("DROP PUBLICATION IF EXISTS " + PROBE_PUBLICATION);
            // 대용량 픽스처를 남기면 다음 회차의 유휴 잡음과 디스크에 계속 얹힌다.
            Db.onSource("TRUNCATE TABLE verify_wal_wide, verify_wal_narrow, verify_toast_probe RESTART IDENTITY");
        } catch (RuntimeException e) {
            System.out.println("[" + ITEM + "] 정리 중 무시된 오류: " + e.getMessage());
        }
    }

    // ── 1단계. 정적 선별 ───────────────────────────────────────────────────

    @Test
    @Order(1)
    @DisplayName("선별 질의가 관심 필드만 후보로 집는다")
    void candidateQueryPicksOnlyTheLargeColumn() throws IOException {
        Db.onSource("TRUNCATE TABLE verify_toast_probe RESTART IDENTITY");
        insertProbeRow("V5B-PROBE", payload("probe"));

        List<Candidate> all = runCandidateQuery();
        Map<String, Candidate> probe = new LinkedHashMap<>();
        all.stream().filter(c -> PROBE_TABLE.equals(c.table())).forEach(c -> probe.put(c.column(), c));

        VerificationReport.metric(ITEM, "선별 질의", CANDIDATE_SQL.getFileName().toString()
                + " (임계 " + TOAST_THRESHOLD + " bytes)");
        VerificationReport.metric(ITEM, "probe · big_blob 최대 저장 크기",
                probe.get("big_blob").maxBytes() + " bytes → 후보 " + probe.get("big_blob").candidate());
        VerificationReport.metric(ITEM, "probe · small_note 최대 저장 크기",
                probe.get("small_note").maxBytes() + " bytes → 후보 " + probe.get("small_note").candidate());

        assertThat(probe.get("big_blob").candidate()).as("대용량 컬럼은 후보로 잡혀야 한다").isTrue();
        assertThat(probe.get("small_note").candidate()).as("작은 컬럼까지 잡으면 규칙이 쓸모없다").isFalse();
    }

    @Test
    @Order(2)
    @DisplayName("운영 스키마 전수 조사 — publication 에 든 테이블에 후보가 있는가")
    void productionSchemaHasNoCandidate() throws IOException {
        List<Candidate> all = runCandidateQuery();
        List<String> published = Db.rowsOnSource(
                        "SELECT tablename FROM pg_publication_tables WHERE pubname = 'embedded_cdc_pub'")
                .stream().map(r -> String.valueOf(r[0])).toList();

        List<Candidate> inScope = all.stream().filter(c -> published.contains(c.table())).toList();
        List<Candidate> flagged = inScope.stream().filter(Candidate::candidate).toList();

        Candidate widest = inScope.stream().max((a, b) -> Long.compare(a.maxBytes(), b.maxBytes())).orElse(null);

        VerificationReport.metric(ITEM, "캡처 대상 테이블 수", String.valueOf(published.size()));
        VerificationReport.metric(ITEM, "그중 toastable 컬럼 수", String.valueOf(inScope.size()));
        VerificationReport.metric(ITEM, "TOAST 후보 컬럼 수", String.valueOf(flagged.size()));
        VerificationReport.metric(ITEM, "가장 큰 컬럼",
                widest == null ? "없음"
                        : widest.table() + "." + widest.column() + " = " + widest.maxBytes() + " bytes");

        if (flagged.isEmpty()) {
            VerificationReport.note(ITEM, "지금 캡처 대상 스키마에는 TOAST 후보 컬럼이 하나도 없다 — "
                    + "그런데도 네 테이블이 모두 REPLICA IDENTITY FULL 이다. "
                    + "FULL 을 요구한 것은 V5 인데, 정작 V5 가 문제 삼는 컬럼이 이 스키마에는 없다");
        } else {
            VerificationReport.note(ITEM, "후보 컬럼: " + flagged.stream()
                    .map(c -> c.table() + "." + c.column() + "(" + c.maxBytes() + "B)").toList());
        }
    }

    // ── 2단계. 동적 확정 ───────────────────────────────────────────────────

    @Test
    @Order(3)
    @DisplayName("후보로 집힌 컬럼만 실제로 자리표시자로 온다 — 규칙이 동작을 예측한다")
    void candidateRulePredictsPlaceholder() throws IOException {
        Db.onSource("TRUNCATE TABLE verify_toast_probe RESTART IDENTITY");
        Db.onSource("ALTER TABLE verify_toast_probe REPLICA IDENTITY DEFAULT");
        recreatePublication(PROBE_PUBLICATION, PROBE_TABLE);
        dropSlotQuietly(PROBE_SLOT);

        Path offsetFile = Files.createTempDirectory("v5b-offset").resolve("offsets.dat");
        try (CaptureHarness harness = new CaptureHarness(
                PROBE_SLOT, PROBE_PUBLICATION, "public." + PROBE_TABLE, offsetFile, new Properties()).start()) {

            long deadline = System.currentTimeMillis() + 30_000;
            while (!slotExists(PROBE_SLOT) && System.currentTimeMillis() < deadline) {
                sleep(200);
            }
            assertThat(slotExists(PROBE_SLOT)).as("probe 슬롯").isTrue();
            sleep(1500);

            insertProbeRow("V5B-DYNAMIC", payload("dynamic"));
            assertThat(harness.poll(Duration.ofSeconds(20))).as("선행 INSERT 이벤트").isNotNull();

            // 관심 필드를 건드리지 않는 UPDATE — 이것이 2단계 확정 절차의 전부다
            Db.onSource("UPDATE verify_toast_probe SET small_note = 'V5B-DYNAMIC-CHANGED'");
            ChangeEvent update = harness.poll(Duration.ofSeconds(20));
            assertThat(update).as("UPDATE 이벤트").isNotNull();

            String big = update.after().values().get("big_blob");
            String small = update.after().values().get("small_note");

            boolean bigUnavailable = TOAST_PLACEHOLDER.equals(big) || big == null;
            boolean smallReadable = small != null && !TOAST_PLACEHOLDER.equals(small);

            VerificationReport.metric(ITEM, "확정 · big_blob(후보) 판독 불가", String.valueOf(bigUnavailable));
            VerificationReport.metric(ITEM, "확정 · small_note(비후보) 판독 가능", String.valueOf(smallReadable));

            assertThat(bigUnavailable).as("후보로 집은 컬럼은 실제로 자리표시자로 와야 규칙이 맞는 것이다").isTrue();
            assertThat(smallReadable).as("비후보 컬럼은 정상적으로 읽혀야 한다").isTrue();

            VerificationReport.note(ITEM, "판별 절차 확정 — 1단계 " + CANDIDATE_SQL.getFileName()
                    + " 로 후보를 뽑고, 2단계로 그 테이블에 관심 필드를 건드리지 않는 UPDATE 를 한 건 흘려 "
                    + "자리표시자가 오는지 본다. 1단계만으로 확정하지 않는 이유는 토스터가 컬럼이 아니라 "
                    + "행 단위로 판단하기 때문이다");
        } finally {
            dropSlotQuietly(PROBE_SLOT);
        }
    }

    // ── FULL 의 대가 ───────────────────────────────────────────────────────

    @Test
    @Order(4)
    @DisplayName("대용량 컬럼이 없는 테이블에서 FULL 이 얼마를 더 쓰는가")
    void walCostOnNarrowTable() {
        // 판정에 쓰는 값이 아니라, 아래 수치에 얼마만큼의 잡음이 섞였는지 보이기 위한 바닥값이다
        long idleFloor = measure(() -> { }).bytes();
        VerificationReport.metric(ITEM, "유휴 바닥값(측정 구간당)", bytes(idleFloor));
        VerificationReport.note(ITEM, "아래 수치에는 위 유휴값만큼의 잡음이 섞여 있다 — WAL 은 클러스터 공용이고 "
                + "운영 파이프라인의 heartbeat 가 계속 돌기 때문이다");

        Costs def = measureTable(NARROW, "DEFAULT", NARROW_ROWS);
        Costs full = measureTable(NARROW, "FULL", NARROW_ROWS);
        report(NARROW, def, full);

        assertThat(full.update().perRow())
                .as("FULL 은 UPDATE 마다 변경 전 행을 더 쓴다")
                .isGreaterThan(def.update().perRow());
        assertThat(full.delete().perRow())
                .as("FULL 은 DELETE 에도 변경 전 행 전체를 쓴다")
                .isGreaterThan(def.delete().perRow());
        assertThat(full.insert().perRow())
                .as("INSERT 는 REPLICA IDENTITY 와 무관하다 — 새 행에는 '변경 전 행'이 없다")
                .isLessThan(def.insert().perRow() * 1.5);

        VerificationReport.note(ITEM, "INSERT 비용은 두 설정이 사실상 같다. V2 의 165 bytes/변경 은 INSERT 부하로 잰 값이라 "
                + "REPLICA IDENTITY 와 무관하다 — FULL 의 대가는 UPDATE/DELETE 에서만 나타난다");
    }

    @Test
    @Order(5)
    @DisplayName("대용량 컬럼이 있는 테이블에서 FULL 이 얼마를 더 쓰는가")
    void walCostOnWideTable() {
        Costs def = measureTable(WIDE, "DEFAULT", WIDE_ROWS);
        Costs full = measureTable(WIDE, "FULL", WIDE_ROWS);
        report(WIDE, def, full);

        assertThat(def.update().perRow())
                .as("DEFAULT 에서는 안 바뀐 TOAST 값이 WAL 에 실리지 않는다 — 그래서 이벤트에서도 빠진다")
                .isLessThan(PAYLOAD_SIZE / 10.0);
        assertThat(full.update().perRow())
                .as("FULL 은 안 바뀐 TOAST 값까지 꺼내서(detoast) WAL 에 넣는다")
                .isGreaterThan((double) PAYLOAD_SIZE);

        VerificationReport.note(ITEM, "대용량 컬럼이 있는 테이블에서 FULL 을 켜면 그 컬럼을 건드리지 않는 UPDATE 한 건이 "
                + "컬럼 크기만큼의 WAL 을 만든다. V5 를 해소하는 대가가 V2 의 디스크 압력으로 그대로 넘어가는 지점이다");
    }

    // ── 테이블별 판단 ──────────────────────────────────────────────────────

    @Test
    @Order(6)
    @DisplayName("테이블별 판단 — DEFAULT 로 내리면 무엇을 잃는가")
    void perTableIdentityJudgement() {
        requireServiceRunning();
        VerificationReport.note(ITEM, "여기서는 운영 테이블과 기동 중인 emb-cdc-service 를 쓴다 — "
                + "\"내려도 되는가\"는 이 파이프라인이 그 테이블의 before 이미지를 실제로 무엇에 쓰는지에 "
                + "달려 있어서 복제본으로는 답이 나오지 않는다");
        computerStillSyncsWithDefault();
        carLosesFieldLevelAuditWithDefault();
    }

    /**
     * computer 는 before 에서 PK 만 읽는다(소프트 삭제). DEFAULT 도 PK 는 싣는다.
     * 그렇다면 DEFAULT 로 내려도 동기화가 유지되어야 한다 — 코드를 읽어 그렇게 보이는 것과
     * 실제로 그런 것은 다르므로, 파이프라인에 흘려서 확인한다.
     */
    private static void computerStillSyncsWithDefault() {
        Db.onSource("ALTER TABLE computer REPLICA IDENTITY DEFAULT");
        Long inserted = null;
        try {
            inserted = Db.scalarOnSource("INSERT INTO computer (brand, model, cpu, ram_gb, price_usd) "
                    + "VALUES ('V5B', 'V5B-INS', 'V5BCPU', 8, 100) RETURNING id", Long.class);
            final long rowId = inserted;

            String created = await("computer INSERT 반영", PROPAGATION_TIMEOUT, () -> Db.scalarOnTarget(
                    "SELECT full_name FROM computer WHERE id = ?", String.class, rowId));

            Db.onSource("UPDATE computer SET model = 'V5B-UPD' WHERE id = " + rowId);
            String updated = await("computer UPDATE 반영", PROPAGATION_TIMEOUT, () -> {
                String v = Db.scalarOnTarget(
                        "SELECT full_name FROM computer WHERE id = ?", String.class, rowId);
                return "V5B V5B-UPD".equals(v) ? v : null;
            });

            Db.onSource("DELETE FROM computer WHERE id = " + rowId);
            Boolean softDeleted = await("computer DELETE(소프트 삭제) 반영", PROPAGATION_TIMEOUT, () -> {
                Boolean d = Db.scalarOnTarget(
                        "SELECT deleted FROM computer WHERE id = ?", Boolean.class, rowId);
                return Boolean.TRUE.equals(d) ? d : null;
            });

            VerificationReport.metric(ITEM, "computer · DEFAULT 에서 INSERT 반영", created);
            VerificationReport.metric(ITEM, "computer · DEFAULT 에서 UPDATE 반영", updated);
            VerificationReport.metric(ITEM, "computer · DEFAULT 에서 소프트 삭제", String.valueOf(softDeleted));

            assertThat(updated).isEqualTo("V5B V5B-UPD");
            assertThat(softDeleted).isTrue();

            VerificationReport.note(ITEM, "computer 는 DEFAULT 로 내려도 동기화가 유지된다 — "
                    + "핸들러가 before 에서 PK 만 읽고, TOAST 후보 컬럼도 없기 때문이다");
        } finally {
            // 되돌리는 것이 이 시나리오의 일부다. 남겨 두면 다음 회차의 전제가 조용히 달라진다.
            Db.onSource("ALTER TABLE computer REPLICA IDENTITY FULL");
            if (inserted != null) {
                Db.onTarget("DELETE FROM computer WHERE id = " + inserted);
            }
        }
    }

    /**
     * car 는 감사 대상이라 before 를 값 비교에 쓴다. DEFAULT 면 before 에 PK 만 실려
     * 비교 자체가 성립하지 않는다 — 그때 감사 로그가 어떻게 남는지를 본다.
     */
    private static void carLosesFieldLevelAuditWithDefault() {
        Long inserted = null;
        try {
            inserted = Db.scalarOnSource(
                    "INSERT INTO car (name, brand, price) VALUES ('V5B-AUDIT', 'V5B', 1000) RETURNING id",
                    Long.class);
            final long rowId = inserted;
            final String rowKey = String.valueOf(rowId);
            await("car INSERT 반영", PROPAGATION_TIMEOUT, () -> Db.scalarOnTarget(
                    "SELECT name FROM car WHERE id = ?", String.class, rowId));

            Db.onSource("ALTER TABLE car REPLICA IDENTITY DEFAULT");
            sleep(500);
            Db.onSource("UPDATE car SET price = price + 1, updated_at = now() WHERE id = " + rowId);
            Boolean identifiableOnDefault = await("DEFAULT 에서의 감사 기록", PROPAGATION_TIMEOUT,
                    () -> auditFlag(rowKey, 1));

            Db.onSource("ALTER TABLE car REPLICA IDENTITY FULL");
            sleep(500);
            Db.onSource("UPDATE car SET price = price + 1, updated_at = now() WHERE id = " + rowId);
            Boolean identifiableOnFull = await("FULL 에서의 감사 기록", PROPAGATION_TIMEOUT,
                    () -> auditFlag(rowKey, 2));

            String changedOnFull = Db.scalarOnTarget(
                    "SELECT changed_fields FROM cdc_change_audit WHERE source_table = 'car' AND row_key = ? "
                            + "ORDER BY id DESC LIMIT 1", String.class, rowKey);

            VerificationReport.metric(ITEM, "car · DEFAULT 에서 필드 단위 변경 식별",
                    String.valueOf(identifiableOnDefault));
            VerificationReport.metric(ITEM, "car · FULL 에서 필드 단위 변경 식별",
                    String.valueOf(identifiableOnFull) + " (변경 필드 " + changedOnFull + ")");

            assertThat(identifiableOnDefault)
                    .as("DEFAULT 면 before 에 PK 만 실려 비교가 성립하지 않는다")
                    .isFalse();
            assertThat(identifiableOnFull)
                    .as("FULL 이면 before/after 비교가 성립한다")
                    .isTrue();

            VerificationReport.note(ITEM, "car 의 FULL 을 요구하는 것은 V5 가 아니라 V1 이다 — "
                    + "감사 대상 테이블은 필드 단위 변경 식별에 before 이미지가 필요하다. "
                    + "TOAST 후보가 없어도 이 테이블은 FULL 을 내릴 수 없다");
        } finally {
            Db.onSource("ALTER TABLE car REPLICA IDENTITY FULL");
            if (inserted != null) {
                Db.onSource("DELETE FROM car WHERE id = " + inserted);
                sleep(2000);
                Db.onTarget("DELETE FROM car WHERE id = " + inserted);
                Db.onTarget("DELETE FROM cdc_change_audit WHERE source_table = 'car' AND row_key = '"
                        + inserted + "'");
            }
        }
    }

    /** 그 행의 감사 기록이 minCount 건 이상 쌓였을 때 가장 최근 건의 identifiable 을 준다. */
    private static Boolean auditFlag(String rowKey, long minCount) {
        Long n = Db.scalarOnTarget(
                "SELECT count(*) FROM cdc_change_audit WHERE source_table = 'car' AND row_key = ?",
                Long.class, rowKey);
        if (n == null || n < minCount) {
            return null;
        }
        return Db.scalarOnTarget(
                "SELECT identifiable FROM cdc_change_audit WHERE source_table = 'car' AND row_key = ? "
                        + "ORDER BY id DESC LIMIT 1", Boolean.class, rowKey);
    }

    // ── 측정 ───────────────────────────────────────────────────────────────

    private record Wal(long bytes, long records, long fpi) {
        Wal minus(Wal other) {
            return new Wal(bytes - other.bytes, records - other.records, fpi - other.fpi);
        }
    }

    private record Sample(Wal wal, int rows) {
        long bytes() {
            return wal.bytes();
        }

        double perRow() {
            return (double) wal.bytes() / rows;
        }
    }

    private record Costs(String identity, Sample insert, Sample update, Sample delete) {
    }

    /**
     * 한 구간의 WAL 사용량을 잰다.
     *
     * 직전에 CHECKPOINT 를 건다 — full page image 는 체크포인트 후 처음 만지는 페이지에만 붙으므로,
     * 걸어 두지 않으면 어느 쪽 측정에 FPI 가 섞였는지에 따라 결과가 흔들린다. 걸어 두면 두 설정이
     * 같은 조건에서 비교된다. FPI 자체는 pg_stat_wal 로 따로 세어 함께 남긴다.
     */
    private static Sample measure(int rows, Runnable work) {
        Db.onSource("CHECKPOINT");
        Wal before = walSnapshot();
        work.run();
        Wal after = walSnapshot();
        return new Sample(after.minus(before), rows);
    }

    private static Sample measure(Runnable work) {
        return measure(1, work);
    }

    /** pg_stat_wal 은 최소 1초 주기로 플러시된다. 그 전에 읽으면 직전 부하가 빠진 값이 나온다. */
    private static Wal walSnapshot() {
        sleep(1100);
        Object[] row = Db.rowsOnSource("""
                SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint AS total,
                       (SELECT wal_records FROM pg_stat_wal)                AS records,
                       (SELECT wal_fpi FROM pg_stat_wal)                    AS fpi
                """).get(0);
        return new Wal(((Number) row[0]).longValue(),
                ((Number) row[1]).longValue(),
                ((Number) row[2]).longValue());
    }

    private static Costs measureTable(String table, String identity, int rows) {
        Db.onSource("ALTER TABLE " + table + " REPLICA IDENTITY " + identity);
        Db.onSource("TRUNCATE TABLE " + table + " RESTART IDENTITY");

        Sample insert = measure(rows, () -> insertRows(table, rows));
        Sample update = measure(rows, () -> Db.onSource(updateSql(table)));
        Sample delete = measure(rows, () -> Db.onSource("DELETE FROM " + table));
        return new Costs(identity, insert, update, delete);
    }

    private static String updateSql(String table) {
        // 두 경우 모두 대용량 컬럼은 건드리지 않는다. 그것이 이 시나리오의 전제다.
        return NARROW.equals(table)
                ? "UPDATE " + NARROW + " SET amount = amount + 1, updated_at = now()"
                : "UPDATE " + WIDE + " SET code = code || 'x'";
    }

    private static void report(String table, Costs def, Costs full) {
        record Row(String op, Sample d, Sample f) {
        }
        List<Row> rows = List.of(
                new Row("INSERT", def.insert(), full.insert()),
                new Row("UPDATE", def.update(), full.update()),
                new Row("DELETE", def.delete(), full.delete()));

        for (Row r : rows) {
            VerificationReport.metric(ITEM, table + " · " + r.op() + " · DEFAULT",
                    perRow(r.d()) + "  (총 " + bytes(r.d().bytes())
                            + ", FPI " + r.d().wal().fpi() + "개)");
            VerificationReport.metric(ITEM, table + " · " + r.op() + " · FULL",
                    perRow(r.f()) + "  (총 " + bytes(r.f().bytes())
                            + ", FPI " + r.f().wal().fpi() + "개)");
            VerificationReport.metric(ITEM, table + " · " + r.op() + " · 증가율",
                    ratio(r.d().perRow(), r.f().perRow()));
        }
    }

    private static String perRow(Sample s) {
        return String.format(Locale.ROOT, "%,.0f bytes/행", s.perRow());
    }

    private static String bytes(long b) {
        if (b < 1024) {
            return b + " B";
        }
        if (b < 1024 * 1024) {
            return String.format(Locale.ROOT, "%.1f KiB", b / 1024.0);
        }
        return String.format(Locale.ROOT, "%.1f MiB", b / (1024.0 * 1024));
    }

    private static String ratio(double base, double other) {
        if (base <= 0) {
            return "산출 불가";
        }
        return String.format(Locale.ROOT, "%.2f 배", other / base);
    }

    // ── 데이터 투입 ────────────────────────────────────────────────────────

    private static void insertRows(String table, int rows) {
        if (NARROW.equals(table)) {
            insertNarrow(rows);
        } else {
            insertWide(rows);
        }
    }

    private static void insertNarrow(int rows) {
        Db.inSourceTransaction(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + NARROW + " (code, amount, status) VALUES (?, ?, ?)")) {
                for (int i = 1; i <= rows; i++) {
                    ps.setString(1, "V5B-" + i);
                    ps.setBigDecimal(2, BigDecimal.valueOf(1000L + i));
                    ps.setString(3, "NEW");
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (SQLException e) {
                throw new IllegalStateException("narrow 적재 실패", e);
            }
        });
    }

    private static void insertWide(int rows) {
        // 같은 문자열을 재사용한다 — EXTERNAL 저장은 중복을 합치지 않으므로 행마다 온전히 쌓인다
        String blob = payload("wide");
        Db.inSourceTransaction(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + WIDE + " (code, big_blob) VALUES (?, ?)")) {
                for (int i = 1; i <= rows; i++) {
                    ps.setString(1, "V5B-" + i);
                    ps.setString(2, blob);
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (SQLException e) {
                throw new IllegalStateException("wide 적재 실패", e);
            }
        });
    }

    private static void insertProbeRow(String note, String blob) {
        try (var c = openSource();
             var ps = c.prepareStatement(
                     "INSERT INTO " + PROBE_TABLE + " (small_note, big_blob) VALUES (?, ?)")) {
            ps.setString(1, note);
            ps.setString(2, blob);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new IllegalStateException("probe 행 삽입 실패", e);
        }
    }

    /** 압축이 잘 되지 않도록 문자를 섞는다. 잘 압축되면 인라인에 남아 TOAST 상황이 재현되지 않는다. */
    private static String payload(String seed) {
        StringBuilder sb = new StringBuilder(PAYLOAD_SIZE);
        String alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
        int k = seed.hashCode();
        for (int i = 0; i < PAYLOAD_SIZE; i++) {
            k = k * 1103515245 + 12345;
            sb.append(alphabet.charAt(Math.abs(k >> 16) % alphabet.length()));
        }
        return sb.toString();
    }

    // ── 선별 질의 실행 ─────────────────────────────────────────────────────

    private record Candidate(String table, String column, String storage, long maxBytes, boolean candidate) {
    }

    private static List<Candidate> runCandidateQuery() throws IOException {
        String sql = Files.readString(CANDIDATE_SQL, StandardCharsets.UTF_8);
        List<Candidate> out = new ArrayList<>();
        for (Object[] row : Db.rowsOnSource(sql)) {
            out.add(new Candidate(
                    String.valueOf(row[1]),
                    String.valueOf(row[2]),
                    String.valueOf(row[4]),
                    ((Number) row[5]).longValue(),
                    Boolean.TRUE.equals(row[6])));
        }
        return out;
    }
}
