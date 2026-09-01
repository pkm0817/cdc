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
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V2. Provider Down Case
 *
 * 통과 기준
 *   - 꺼져 있던 구간의 변경분이 유실 0건으로 도착한다
 *   - 꺼져 있는 동안 쌓이는 WAL 량을 측정해 허용 다운타임을 산출한다
 */
@DisplayName("V2. Provider 다운 후 재기동")
class V2ProviderDownTest extends VerificationSupport {

    private static final String SLOT = "verify_v2_slot";
    private static final int CHANGES_WHILE_DOWN = 1_000;

    private static Path offsetFile;

    @BeforeAll
    static void prepare() throws IOException {
        VerificationReport.section("V2. Provider 다운 후 재기동");
        createRecordFixture();
        truncateRecords();
        dropSlotQuietly(SLOT);
        offsetFile = Files.createTempDirectory("v2-offset").resolve("offsets.dat");
    }

    @AfterAll
    static void cleanup() {
        dropSlotQuietly(SLOT);
    }

    @Test
    @DisplayName("꺼져 있던 구간의 변경분이 하나도 빠지지 않고 도착한다")
    void noLossAcrossRestart() {
        // ── 1차 기동: 슬롯을 만들고 오프셋을 남긴다 ────────────────────────
        CaptureHarness first = new CaptureHarness(
                SLOT, RECORD_PUBLICATION, "public." + RECORD_TABLE, offsetFile, new Properties()).start();
        awaitSlot();
        sleep(1500);

        insertRecord("V2-BEFORE-1", new BigDecimal("100.00"), "NEW");
        assertThat(first.poll(Duration.ofSeconds(10))).as("1차 기동 중 캡처").isNotNull();

        // 오프셋이 파일에 flush 될 시간을 준다 (flush.interval 1초)
        sleep(2000);
        long retainedBeforeDown = slotRetainedWalBytes(SLOT);

        // ── 정지 ──────────────────────────────────────────────────────────
        first.stopGracefully();
        assertThat(slotActive(SLOT)).as("정지하면 슬롯 점유가 풀린다").isFalse();
        VerificationReport.note("V2", "Provider 정지 — 슬롯은 남아 있고 active=false 가 된다");

        // ── 꺼져 있는 동안 변경 발생 ───────────────────────────────────────
        long downStart = System.currentTimeMillis();
        insertRecordsInOneTransaction("V2-DOWN", CHANGES_WHILE_DOWN);
        // WAL 이 실제로 쌓이도록 잠깐 둔다
        sleep(2000);
        long retainedWhileDown = slotRetainedWalBytes(SLOT);
        long downMs = System.currentTimeMillis() - downStart;

        long walGrowth = retainedWhileDown - Math.max(retainedBeforeDown, 0);
        VerificationReport.metric("V2", "다운 중 변경 건수", String.valueOf(CHANGES_WHILE_DOWN));
        VerificationReport.metric("V2", "다운 시간", downMs + " ms");
        VerificationReport.metric("V2", "슬롯이 붙잡은 WAL", humanBytes(retainedWhileDown));
        VerificationReport.metric("V2", "변경 1건당 WAL 증가",
                (CHANGES_WHILE_DOWN == 0 ? 0 : walGrowth / CHANGES_WHILE_DOWN) + " bytes");

        String keepSize = Db.scalarOnSource("SHOW max_slot_wal_keep_size", String.class);
        VerificationReport.metric("V2", "max_slot_wal_keep_size", String.valueOf(keepSize));
        if ("-1".equals(keepSize)) {
            VerificationReport.note("V2",
                    "max_slot_wal_keep_size=-1 — 상한이 없다. Provider 가 오래 죽으면 WAL 이 디스크를 채울 때까지 쌓인다");
        }

        // ── 재기동 ────────────────────────────────────────────────────────
        CaptureHarness second = new CaptureHarness(
                SLOT, RECORD_PUBLICATION, "public." + RECORD_TABLE, offsetFile, new Properties()).start();

        List<ChangeEvent> replayed = second.collect(CHANGES_WHILE_DOWN, Duration.ofMinutes(3));
        second.stopGracefully();

        Set<String> receivedKeys = replayed.stream()
                .filter(e -> e.after() != null)
                .map(e -> e.after().text("biz_key"))
                .filter(k -> k.startsWith("V2-DOWN-"))
                .collect(Collectors.toCollection(TreeSet::new));

        Set<String> expectedKeys = new TreeSet<>();
        for (int i = 1; i <= CHANGES_WHILE_DOWN; i++) {
            expectedKeys.add("V2-DOWN-" + i);
        }

        Set<String> missing = new TreeSet<>(expectedKeys);
        missing.removeAll(receivedKeys);

        VerificationReport.metric("V2", "재기동 후 수신", receivedKeys.size() + " / " + CHANGES_WHILE_DOWN);
        VerificationReport.metric("V2", "유실 건수", String.valueOf(missing.size()));

        assertThat(missing).as("꺼져 있던 구간의 변경분은 유실 0건이어야 한다").isEmpty();
        VerificationReport.note("V2", "재기동 시 마지막 오프셋부터 이어 읽어 유실 없음 — 슬롯이 WAL 을 붙잡아 준 결과");
    }

    private static void awaitSlot() {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!slotExists(SLOT) && System.currentTimeMillis() < deadline) {
            sleep(200);
        }
        assertThat(slotExists(SLOT)).isTrue();
    }

    private static String humanBytes(long bytes) {
        if (bytes < 0) {
            return "측정 불가";
        }
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format("%.1f KiB", bytes / 1024.0);
        }
        return String.format("%.2f MiB", bytes / (1024.0 * 1024.0));
    }
}
