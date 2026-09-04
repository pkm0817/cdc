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
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V3. 캡처 연결고리가 끊기는 경우 — 복구 불가능 구간을 탐지하는가
 *
 * 슬롯이 사라지면 그 사이 WAL 은 PostgreSQL 이 지워 버릴 수 있고, 되받을 방법이 없다.
 * 그러므로 "복구"가 아니라 "탐지"가 통과 기준이다. 탐지하지 못하면 조용히 어긋난 채로 운영된다.
 *
 * 통과 기준
 *   - 유실 발생을 자동 탐지한다
 *   - 재동기화 절차로 정합을 복원할 수 있다
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("V3. 복제 슬롯 유실")
class V3SlotLossTest extends VerificationSupport {

    private static final String SLOT = "verify_v3_slot";
    private static final int CHANGES_IN_GAP = 100;

    private static Path offsetFile;
    private static String lastConfirmedLsn;

    @BeforeAll
    static void prepare() throws IOException {
        VerificationReport.section("V3. 복제 슬롯 유실");
        createRecordFixture();
        truncateRecords();
        dropSlotQuietly(SLOT);
        offsetFile = Files.createTempDirectory("v3-offset").resolve("offsets.dat");
    }

    @AfterAll
    static void cleanup() {
        dropSlotQuietly(SLOT);
    }

    @Test
    @Order(1)
    @DisplayName("슬롯이 삭제되면 엔진이 기동을 거부한다 — 조용히 넘어가지 않는다")
    void slotLossIsDetectedByEngine() {
        CaptureHarness first = new CaptureHarness(
                SLOT, RECORD_PUBLICATION, "public." + RECORD_TABLE, offsetFile, new Properties()).start();
        awaitSlot();
        sleep(1500);

        insertRecord("V3-BEFORE-1", new BigDecimal("100.00"), "NEW");
        assertThat(first.poll(Duration.ofSeconds(10))).as("정상 캡처 확인").isNotNull();
        sleep(2000); // 오프셋 flush

        lastConfirmedLsn = slotConfirmedFlushLsn(SLOT);
        VerificationReport.metric("V3", "슬롯 삭제 직전 confirmed_flush_lsn", String.valueOf(lastConfirmedLsn));

        first.stopGracefully();

        // ── 연결고리 절단 ─────────────────────────────────────────────────
        dropSlotQuietly(SLOT);
        assertThat(slotExists(SLOT)).as("슬롯이 사라진 상태를 만든다").isFalse();
        // 같은 상태가 되는 경로를 정확히 적는다. 재기동은 여기 들지 않는다 —
        // 논리 슬롯은 pg_replslot/ 에 디스크로 남아 정상 재기동과 크래시 복구를 살아남는다
        // (scripts/v3b-failover.sh 의 A-1·A-2 실측). 슬롯이 사라지는 것은 데이터 디렉터리를
        // 버릴 때(A-3)와 페일오버할 때(B-2)다. PG16 은 논리 슬롯을 standby 로 동기화하지 않는다.
        VerificationReport.note("V3", "복제 슬롯 강제 삭제 — 볼륨이 사라지는 재기동이나 "
                + "이중화 전환(페일오버)에서도 같은 상태가 된다. 정상 재기동으로는 슬롯이 사라지지 않는다");

        // ── 되받을 수 없는 구간의 변경 ────────────────────────────────────
        insertRecordsInOneTransaction("V3-GAP", CHANGES_IN_GAP);
        sleep(1000);

        // ── 재기동 ────────────────────────────────────────────────────────
        CaptureHarness second = new CaptureHarness(
                SLOT, RECORD_PUBLICATION, "public." + RECORD_TABLE, offsetFile, new Properties()).start();
        sleep(8000); // 슬롯 재생성과 스트리밍 진입까지 기다린다

        List<ChangeEvent> received = second.collect(CHANGES_IN_GAP, Duration.ofSeconds(20));

        Set<String> receivedKeys = received.stream()
                .filter(e -> e.after() != null)
                .map(e -> e.after().text("biz_key"))
                .filter(k -> k.startsWith("V3-GAP-"))
                .collect(Collectors.toCollection(TreeSet::new));

        boolean engineFailed = second.engineFailure() != null;
        String failureMessage = second.engineFailureMessage();
        boolean slotRecreated = slotExists(SLOT);

        VerificationReport.metric("V3", "끊긴 구간 변경 건수", String.valueOf(CHANGES_IN_GAP));
        VerificationReport.metric("V3", "재기동 후 수신", receivedKeys.size() + " / " + CHANGES_IN_GAP);
        VerificationReport.metric("V3", "엔진이 오류로 종료했는가", String.valueOf(engineFailed));
        VerificationReport.metric("V3", "엔진 종료 메시지", String.valueOf(failureMessage));
        VerificationReport.metric("V3", "슬롯이 새로 만들어졌는가", String.valueOf(slotRecreated));

        int lost = CHANGES_IN_GAP - receivedKeys.size();
        VerificationReport.metric("V3", "유실 건수", String.valueOf(lost));

        second.stopGracefully();

        // 슬롯이 사라진 구간을 되받을 방법은 없다. 유실 자체는 막을 수 없는 것이므로
        // 이 항목의 통과 기준은 "유실이 나지 않는 것"이 아니라 "유실을 알아채는 것"이다.
        assertThat(lost).as("슬롯이 사라진 구간은 되받을 수 없다 — 유실이 나는 것 자체는 예상된 결과다")
                .isEqualTo(CHANGES_IN_GAP);

        assertThat(engineFailed)
                .as("엔진이 조용히 새 슬롯을 만들고 넘어가면 유실을 인지할 수 없다. "
                        + "저장된 오프셋을 더 이상 읽을 수 없다는 사실을 오류로 알려야 한다")
                .isTrue();
        assertThat(failureMessage).contains("no longer available on the server");
        assertThat(slotRecreated)
                .as("기동을 거부했으므로 슬롯도 새로 만들어지지 않는다").isFalse();

        VerificationReport.note("V3",
                "Debezium 이 기동을 거부하며 오류로 종료한다 — 유실 구간이 조용히 넘어가지 않는다. "
                        + "다만 프로세스는 재시작 루프에 들어가므로 운영에서는 이 예외를 잡아 경보로 올려야 한다");
    }

    @Test
    @Order(2)
    @DisplayName("오프셋과 슬롯을 대조하면 유실을 기동 시점에 탐지할 수 있다")
    void continuityGuardDetectsGap() {
        // 엔진이 알려 주지 않으므로, 애플리케이션이 기동할 때 직접 확인해야 한다.
        // 판정 규칙: 저장된 오프셋의 LSN 이 슬롯의 restart_lsn 보다 뒤에 있으면 그 사이는 되받을 수 없다.
        boolean slotPresent = slotExists(SLOT);
        String currentRestartLsn = Db.scalarOnSource(
                "SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = ?",
                String.class, SLOT);

        // 운영 가드와 같은 규칙: 처리 이력이 있는데 슬롯이 없거나, restart_lsn 이 앞서 있으면 유실이다
        boolean gapDetected = !slotPresent || currentRestartLsn == null
                || SlotContinuityGuard.toLong(currentRestartLsn) > SlotContinuityGuard.toLong(lastConfirmedLsn);

        VerificationReport.metric("V3", "직전 처리 LSN", String.valueOf(lastConfirmedLsn));
        VerificationReport.metric("V3", "현재 슬롯 restart_lsn", String.valueOf(currentRestartLsn));
        VerificationReport.metric("V3", "가드가 유실을 탐지했는가", String.valueOf(gapDetected));

        assertThat(gapDetected)
                .as("슬롯의 restart_lsn 이 마지막 처리 지점보다 앞서 있으면 그 사이는 영영 못 받는다")
                .isTrue();

        VerificationReport.note("V3",
                "권고: 기동 시 저장된 오프셋 LSN 과 슬롯 restart_lsn 을 대조해 "
                        + "역전되면 기동을 거부하고 재동기화를 요구할 것");
    }

    @Test
    @Order(3)
    @DisplayName("재동기화(전량 재적재)로 정합을 복원할 수 있다")
    void resyncRestoresConsistency() {
        // 끊긴 구간은 WAL 로 못 받으므로 원본을 다시 읽는 수밖에 없다.
        // Debezium 의 snapshot 이 하는 일과 같은 것을 검증 테이블에서 재현한다.
        Db.onTarget("""
                CREATE TABLE IF NOT EXISTS verify_record_replica (
                    id         BIGINT        PRIMARY KEY,
                    biz_key    TEXT          NOT NULL,
                    amount     NUMERIC(14,2) NOT NULL,
                    status     TEXT          NOT NULL
                )
                """);
        Db.onTarget("TRUNCATE TABLE verify_record_replica");

        // 유실 상태를 흉내 낸다: 절반만 들어가 있는 상태
        List<Object[]> all = Db.rowsOnSource(
                "SELECT id, biz_key, amount, status FROM verify_record ORDER BY id");
        int half = all.size() / 2;
        for (int i = 0; i < half; i++) {
            Object[] r = all.get(i);
            Db.onTarget("INSERT INTO verify_record_replica (id, biz_key, amount, status) VALUES ("
                    + r[0] + ", '" + r[1] + "', " + r[2] + ", '" + r[3] + "')");
        }

        long beforeResync = countReplica();
        assertThat(beforeResync).as("재동기화 전에는 어긋나 있다").isLessThan(all.size());

        // ── 재동기화 절차: 전량을 멱등 UPSERT 로 다시 적재 ─────────────────
        for (Object[] r : all) {
            Db.onTarget("INSERT INTO verify_record_replica (id, biz_key, amount, status) VALUES ("
                    + r[0] + ", '" + r[1] + "', " + r[2] + ", '" + r[3] + "') "
                    + "ON CONFLICT (id) DO UPDATE SET biz_key = EXCLUDED.biz_key, "
                    + "amount = EXCLUDED.amount, status = EXCLUDED.status");
        }

        long afterResync = countReplica();
        VerificationReport.metric("V3", "재동기화 전 수신측 건수", beforeResync + " / " + all.size());
        VerificationReport.metric("V3", "재동기화 후 수신측 건수", afterResync + " / " + all.size());

        assertThat(afterResync).as("전량 재적재로 정합이 복원된다").isEqualTo(all.size());
        VerificationReport.note("V3",
                "재동기화는 멱등 UPSERT 라 중복 적재가 안전하다. "
                        + "다만 원본에서 사라진 행은 재적재로 지워지지 않으므로 삭제 대사가 따로 필요하다");
    }

    private static long countReplica() {
        Long n = Db.scalarOnTarget("SELECT count(*) FROM verify_record_replica", Long.class);
        return n == null ? 0 : n;
    }

    private static void awaitSlot() {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!slotExists(SLOT) && System.currentTimeMillis() < deadline) {
            sleep(200);
        }
        assertThat(slotExists(SLOT)).isTrue();
    }
}
