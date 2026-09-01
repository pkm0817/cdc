package dev.embeddedcdc.verification;

import dev.embeddedcdc.domain.model.ChangeEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V4. 같은 이벤트가 두 번 오는 경우 — 이중 반영 제한
 *
 * Debezium 은 at-least-once 다. 오프셋을 flush 하기 전에 죽으면 그 구간이 다시 온다.
 * 이것은 결함이 아니라 정상 동작이므로, 막을 대상은 "중복 수신"이 아니라 "중복 반영"이다.
 *
 * 통과 기준
 *   - 중복이 실제로 유입된다(= at-least-once 임을 확인)
 *   - 그럼에도 최종 데이터 정합이 유지된다
 */
@DisplayName("V4. 중복 유입과 이중 반영 방지")
class V4DuplicateDeliveryTest extends VerificationSupport {

    private static final String SLOT = "verify_v4_slot";
    private static final int RECORDS = 200;

    private static Path offsetFile;

    @BeforeAll
    static void prepare() throws IOException {
        VerificationReport.section("V4. 중복 유입과 이중 반영 방지");
        createRecordFixture();
        truncateRecords();
        dropSlotQuietly(SLOT);
        offsetFile = Files.createTempDirectory("v4-offset").resolve("offsets.dat");

        Db.onTarget("""
                CREATE TABLE IF NOT EXISTS verify_sink (
                    id         BIGINT        PRIMARY KEY,
                    biz_key    TEXT          NOT NULL,
                    amount     NUMERIC(14,2) NOT NULL,
                    apply_count INT          NOT NULL DEFAULT 1,
                    source_lsn BIGINT        NOT NULL
                )
                """);
        Db.onTarget("TRUNCATE TABLE verify_sink");
    }

    @AfterAll
    static void cleanup() {
        dropSlotQuietly(SLOT);
    }

    @Test
    @DisplayName("오프셋 flush 전에 죽으면 중복이 오지만 최종 정합은 유지된다")
    void duplicatesDoNotDoubleApply() throws IOException {
        CaptureHarness first = new CaptureHarness(
                SLOT, RECORD_PUBLICATION, "public." + RECORD_TABLE, offsetFile, new Properties()).start();
        awaitSlot();
        sleep(1500);

        // ── 선행 구간: 여기까지는 오프셋이 확실히 기록된 상태로 만든다 ──────
        insertRecordsInOneTransaction("V4-BASE", 20);
        assertThat(first.collect(20, Duration.ofSeconds(60))).hasSize(20);
        sleep(3000); // flush.interval 1초 * 여유

        // "마지막으로 안전하게 기록된 오프셋"을 떠 둔다.
        // 프로세스가 이 시점 이후에 죽었다면 재기동은 여기서부터 다시 읽는다.
        Path checkpoint = Files.createTempFile("v4-offset-checkpoint", ".dat");
        Files.copy(offsetFile, checkpoint, StandardCopyOption.REPLACE_EXISTING);
        VerificationReport.note("V4", "오프셋 체크포인트 확보 — 이 지점 이후는 flush 되지 않은 것으로 간주한다");

        // ── 이 구간의 이벤트가 중복 대상이 된다 ────────────────────────────
        first.clearQueue();
        insertRecordsInOneTransaction("V4", RECORDS);
        List<ChangeEvent> firstRound = first.collect(RECORDS, Duration.ofMinutes(2));
        assertThat(firstRound).as("1차 수신").hasSize(RECORDS);

        int appliedFirst = applyAll(firstRound);
        first.stopGracefully();

        // ── 오프셋을 체크포인트로 되돌린다 ────────────────────────────────
        // kill -9 로 프로세스를 죽이면 복제 연결이 늦게 끊겨 재기동이 슬롯을 잡지 못한다.
        // 재현하려는 것은 "죽는 방식"이 아니라 "오프셋이 뒤처진 채 재기동되는 상태"이므로
        // 파일을 되돌려 그 상태를 결정적으로 만든다. 재생되는 코드 경로는 완전히 같다.
        Files.copy(checkpoint, offsetFile, StandardCopyOption.REPLACE_EXISTING);
        VerificationReport.note("V4", "오프셋을 체크포인트로 되돌림 — flush 전에 죽은 상황과 동일한 상태");

        // ── 재기동: 되돌린 지점부터 다시 온다 ──────────────────────────────
        CaptureHarness second = new CaptureHarness(
                SLOT, RECORD_PUBLICATION, "public." + RECORD_TABLE, offsetFile, new Properties()).start();
        List<ChangeEvent> secondRound = second.collect(RECORDS, Duration.ofMinutes(2));
        second.stopGracefully();
        assertThat(second.engineFailure()).as("재기동은 성공해야 한다").isNull();

        Set<Long> firstLsns = new HashSet<>();
        firstRound.forEach(e -> firstLsns.add(e.lsn()));
        long duplicates = secondRound.stream().filter(e -> firstLsns.contains(e.lsn())).count();

        VerificationReport.metric("V4", "1차 수신", String.valueOf(firstRound.size()));
        VerificationReport.metric("V4", "재기동 후 수신", String.valueOf(secondRound.size()));
        VerificationReport.metric("V4", "그중 중복(같은 LSN)", String.valueOf(duplicates));

        int appliedSecond = applyAll(secondRound);
        VerificationReport.metric("V4", "1차 반영 행 수", String.valueOf(appliedFirst));
        VerificationReport.metric("V4", "2차 반영 행 수", String.valueOf(appliedSecond));

        // ── 최종 정합 판정 ────────────────────────────────────────────────
        Long sinkRows = Db.scalarOnTarget("SELECT count(*) FROM verify_sink", Long.class);
        Long doubleApplied = Db.scalarOnTarget(
                "SELECT count(*) FROM verify_sink WHERE apply_count > 1", Long.class);
        // 선행 구간(V4-BASE-*)은 sink 에 반영하지 않았으므로 비교에서 뺀다
        Long sourceRows = Db.scalarOnSource(
                "SELECT count(*) FROM verify_record WHERE biz_key LIKE 'V4-%' AND biz_key NOT LIKE 'V4-BASE-%'",
                Long.class);

        VerificationReport.metric("V4", "원본 건수", String.valueOf(sourceRows));
        VerificationReport.metric("V4", "수신측 건수", String.valueOf(sinkRows));
        VerificationReport.metric("V4", "두 번 반영된 행", String.valueOf(doubleApplied));

        assertThat(duplicates).as("at-least-once 이므로 중복이 실제로 와야 한다").isGreaterThan(0);
        assertThat(sinkRows).as("중복이 와도 행이 늘지 않는다").isEqualTo(sourceRows);
        assertThat(doubleApplied).as("같은 실적이 두 번 반영되면 안 된다").isZero();

        VerificationReport.note("V4",
                "LSN 가드가 붙은 멱등 UPSERT 라 중복 이벤트는 갱신 행 수 0 으로 차단된다");
    }

    /**
     * 운영의 ComputerSink 와 같은 규칙으로 반영한다 —
     * 멱등 UPSERT + source_lsn 가드. 더 오래되거나 같은 LSN 은 반영되지 않는다.
     *
     * @return 실제로 반영된 행 수
     */
    private static int applyAll(List<ChangeEvent> events) {
        int applied = 0;
        List<String> statements = new ArrayList<>();
        for (ChangeEvent e : events) {
            if (e.after() == null) {
                continue;
            }
            statements.add("INSERT INTO verify_sink (id, biz_key, amount, apply_count, source_lsn) VALUES ("
                    + e.after().longValue("id") + ", '" + e.after().text("biz_key") + "', "
                    + e.after().decimal("amount") + ", 1, " + e.lsn() + ") "
                    + "ON CONFLICT (id) DO UPDATE SET "
                    + "biz_key = EXCLUDED.biz_key, amount = EXCLUDED.amount, "
                    + "apply_count = verify_sink.apply_count + 1, source_lsn = EXCLUDED.source_lsn "
                    + "WHERE EXCLUDED.source_lsn > verify_sink.source_lsn");
        }
        for (String sql : statements) {
            applied += Db.updateOnTarget(sql); // 가드에 걸리면 0 이 더해진다
        }
        return applied;
    }

    private static void awaitSlot() {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!slotExists(SLOT) && System.currentTimeMillis() < deadline) {
            sleep(200);
        }
        assertThat(slotExists(SLOT)).isTrue();
    }
}
