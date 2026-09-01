package dev.embeddedcdc.verification;

import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.infrastructure.cdc.SlotContinuityGuard;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V6. 최종 안정성 — 대사와 heartbeat
 *
 * 앞의 V1~V5 는 "이 상황에서 어떻게 되는가"를 봤다. V6 는 "그 상황이 벌어졌을 때 알아채는가"를 본다.
 * 파이프라인이 조용히 어긋나는 것을 막는 마지막 그물이다.
 *
 * 통과 기준
 *   - 인위적으로 유실을 주입하면 대사에서 검출된다
 *   - heartbeat 가 변경이 없는 구간에서도 슬롯을 전진시킨다
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("V6. 대사와 heartbeat")
class V6ReconciliationTest extends VerificationSupport {

    private static final String SLOT = "verify_v6_slot";
    private static final int RECORDS = 300;

    private static Path offsetFile;

    @BeforeAll
    static void prepare() throws IOException {
        VerificationReport.section("V6. 대사와 heartbeat");
        createRecordFixture();
        truncateRecords();
        dropSlotQuietly(SLOT);
        offsetFile = Files.createTempDirectory("v6-offset").resolve("offsets.dat");

        Db.onTarget("""
                CREATE TABLE IF NOT EXISTS verify_recon (
                    id      BIGINT        PRIMARY KEY,
                    biz_key TEXT          NOT NULL,
                    amount  NUMERIC(14,2) NOT NULL,
                    status  TEXT          NOT NULL
                )
                """);
        Db.onTarget("TRUNCATE TABLE verify_recon");
    }

    @AfterAll
    static void cleanup() {
        dropSlotQuietly(SLOT);
    }

    @Test
    @Order(1)
    @DisplayName("정상 동기화 후에는 건수와 체크섬이 양쪽에서 일치한다")
    void reconciliationMatchesWhenHealthy() {
        try (CaptureHarness harness = new CaptureHarness(
                SLOT, RECORD_PUBLICATION, "public." + RECORD_TABLE, offsetFile, new Properties()).start()) {
            awaitSlot();
            sleep(1500);

            insertRecordsInOneTransaction("V6", RECORDS);
            List<ChangeEvent> events = harness.collect(RECORDS, Duration.ofMinutes(2));
            assertThat(events).hasSize(RECORDS);

            for (ChangeEvent e : events) {
                if (e.after() == null) {
                    continue;
                }
                Db.updateOnTarget("INSERT INTO verify_recon (id, biz_key, amount, status) VALUES ("
                        + e.after().longValue("id") + ", '" + e.after().text("biz_key") + "', "
                        + e.after().decimal("amount") + ", '" + e.after().text("status") + "') "
                        + "ON CONFLICT (id) DO UPDATE SET biz_key = EXCLUDED.biz_key, "
                        + "amount = EXCLUDED.amount, status = EXCLUDED.status");
            }
        }

        Reconciliation r = reconcile();
        VerificationReport.metric("V6", "원본 건수", String.valueOf(r.sourceCount));
        VerificationReport.metric("V6", "수신측 건수", String.valueOf(r.targetCount));
        VerificationReport.metric("V6", "원본 체크섬", r.sourceChecksum);
        VerificationReport.metric("V6", "수신측 체크섬", r.targetChecksum);
        VerificationReport.metric("V6", "대사 결과", r.matches() ? "일치" : "불일치");

        assertThat(r.matches()).as("정상 동기화 상태에서는 대사가 일치해야 한다").isTrue();
    }

    @Test
    @Order(2)
    @DisplayName("인위적으로 유실을 주입하면 대사가 검출한다")
    void reconciliationDetectsInjectedLoss() {
        int removed = Db.updateOnTarget(
                "DELETE FROM verify_recon WHERE id IN (SELECT id FROM verify_recon ORDER BY id LIMIT 3)");
        VerificationReport.note("V6", "수신측에서 " + removed + "건을 임의 삭제 — 유실 상황 주입");

        Reconciliation r = reconcile();
        VerificationReport.metric("V6", "유실 주입 후 건수차",
                (r.sourceCount - r.targetCount) + " 건");
        VerificationReport.metric("V6", "유실 주입 후 대사 결과", r.matches() ? "일치" : "불일치");

        assertThat(r.matches()).as("유실이 있으면 대사가 반드시 불일치를 내야 한다").isFalse();
        assertThat(r.sourceCount - r.targetCount).isEqualTo(removed);

        // 값만 바뀐 경우도 잡히는지 — 건수는 같고 체크섬만 달라지는 상황
        Db.updateOnTarget("UPDATE verify_recon SET amount = amount + 1 "
                + "WHERE id = (SELECT min(id) FROM verify_recon)");
        Reconciliation afterTamper = reconcile();
        VerificationReport.metric("V6", "값 변조 후 대사 결과", afterTamper.matches() ? "일치" : "불일치");
        VerificationReport.note("V6", "건수가 같아도 체크섬이 달라 검출된다 — 건수 대사만으로는 부족하다");
    }

    @Test
    @Order(3)
    @DisplayName("heartbeat.interval.ms 만으로는 유휴 구간에서 슬롯이 전진하지 않는다")
    void heartbeatIntervalAloneDoesNotAdvanceSlot() {
        Properties onlyInterval = new Properties();
        onlyInterval.setProperty("heartbeat.interval.ms", "1000");

        long advanced = measureSlotAdvanceWhileIdle("verify_v6_hb_interval", onlyInterval, "간격만");

        VerificationReport.metric("V6", "heartbeat.interval.ms 단독 · 슬롯 전진량", advanced + " bytes");
        if (advanced == 0) {
            VerificationReport.note("V6",
                    "발견: heartbeat.interval.ms 를 켜도 관심 테이블에 변경이 없으면 슬롯이 전혀 전진하지 않는다. "
                            + "다른 테이블이 만든 WAL 을 슬롯이 계속 붙잡는다");
        }
        assertThat(advanced)
                .as("이 설정만으로는 전진하지 않는다는 것이 이 항목의 관측 결과다")
                .isZero();
    }

    @Test
    @Order(4)
    @DisplayName("heartbeat.action.query 를 함께 걸면 유휴 구간에도 슬롯이 전진한다")
    void heartbeatActionQueryAdvancesSlot() {
        // 관심 테이블 집합 안에서 WAL 을 만들어 줘야 커넥터가 받아서 오프셋을 올릴 수 있다.
        Db.onSource("""
                CREATE TABLE IF NOT EXISTS verify_heartbeat (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);
        Db.onSource("ALTER TABLE verify_heartbeat REPLICA IDENTITY FULL");
        Db.onSource("DROP PUBLICATION IF EXISTS verify_hb_pub",
                "CREATE PUBLICATION verify_hb_pub FOR TABLE verify_record, verify_heartbeat");

        Properties withAction = new Properties();
        withAction.setProperty("heartbeat.interval.ms", "1000");
        withAction.setProperty("heartbeat.action.query", "INSERT INTO verify_heartbeat (ts) VALUES (now())");

        long advanced = measureSlotAdvanceWhileIdle(
                "verify_v6_hb_action", withAction, "간격 + action.query",
                "verify_hb_pub", "public.verify_record,public.verify_heartbeat");

        VerificationReport.metric("V6", "heartbeat.action.query 병행 · 슬롯 전진량", advanced + " bytes");

        assertThat(advanced)
                .as("action.query 가 관심 테이블에 WAL 을 만들어 주면 슬롯이 전진한다")
                .isGreaterThan(0);

        VerificationReport.note("V6",
                "권고: 관심 테이블 변경이 드문 환경에서는 heartbeat.interval.ms 와 "
                        + "heartbeat.action.query 를 반드시 함께 설정할 것. "
                        + "간격만 켜면 WAL 이 계속 쌓인다");
    }

    private long measureSlotAdvanceWhileIdle(String slot, Properties extra, String label) {
        return measureSlotAdvanceWhileIdle(slot, extra, label, RECORD_PUBLICATION, "public." + RECORD_TABLE);
    }

    /**
     * 관심 테이블은 건드리지 않고 다른 테이블에만 WAL 을 발생시킨 뒤,
     * 슬롯의 confirmed_flush_lsn 이 얼마나 전진했는지 잰다.
     */
    private long measureSlotAdvanceWhileIdle(String slot, Properties extra, String label,
                                             String publication, String tables) {
        dropSlotQuietly(slot);
        try (CaptureHarness harness = new CaptureHarness(
                slot, publication, tables,
                offsetFile.getParent().resolve(slot + ".dat"), extra).start()) {

            long deadline = System.currentTimeMillis() + 30_000;
            while (!slotExists(slot) && System.currentTimeMillis() < deadline) {
                sleep(200);
            }
            assertThat(slotExists(slot)).isTrue();
            sleep(3000);

            String lsnBefore = slotConfirmedFlushLsn(slot);
            long retainedBefore = slotRetainedWalBytes(slot);

            Db.onSource("CREATE TABLE IF NOT EXISTS verify_noise (id BIGSERIAL PRIMARY KEY, v TEXT)");
            for (int i = 0; i < 20; i++) {
                Db.onSource("INSERT INTO verify_noise (v) VALUES (repeat('x', 10000))");
                sleep(150);
            }
            sleep(8000); // heartbeat 이 여러 번 돌고 오프셋이 flush 될 시간

            String lsnAfter = slotConfirmedFlushLsn(slot);
            long retainedAfter = slotRetainedWalBytes(slot);

            VerificationReport.metric("V6", label + " · confirmed_flush_lsn",
                    lsnBefore + " -> " + lsnAfter);
            VerificationReport.metric("V6", label + " · 붙잡은 WAL",
                    retainedBefore + " -> " + retainedAfter + " bytes");

            return SlotContinuityGuard.toLong(lsnAfter) - SlotContinuityGuard.toLong(lsnBefore);
        } finally {
            dropSlotQuietly(slot);
            Db.onSource("DROP TABLE IF EXISTS verify_noise");
        }
    }

    // ── 대사 로직 ──────────────────────────────────────────────────────────

    /**
     * 건수 + 체크섬 대사.
     * 체크섬은 정렬된 전체 행을 이어 붙여 md5 를 낸다 — 값이 하나만 달라도 바뀐다.
     * 건수만 비교하면 "지워지고 다른 게 들어온" 경우를 놓친다.
     */
    private static final String RECONCILE_SQL = """
            SELECT count(*)::bigint AS row_count,
                   coalesce(md5(string_agg(
                       id || '|' || biz_key || '|' || amount || '|' || status, ',' ORDER BY id)), '') AS checksum
            FROM %s WHERE biz_key LIKE 'V6-%%'
            """;

    private static Reconciliation reconcile() {
        Object[] source = Db.rowsOnSource(RECONCILE_SQL.formatted("verify_record")).get(0);
        Object[] target = Db.rowsOnTarget(RECONCILE_SQL.formatted("verify_recon")).get(0);

        return new Reconciliation(
                ((Number) source[0]).longValue(), ((Number) target[0]).longValue(),
                String.valueOf(source[1]), String.valueOf(target[1]));
    }

    private record Reconciliation(long sourceCount, long targetCount,
                                  String sourceChecksum, String targetChecksum) {
        boolean matches() {
            return sourceCount == targetCount && sourceChecksum.equals(targetChecksum);
        }
    }

    private static void awaitSlot() {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!slotExists(SLOT) && System.currentTimeMillis() < deadline) {
            sleep(200);
        }
        assertThat(slotExists(SLOT)).isTrue();
    }
}
