package dev.embeddedcdc.verification;

import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.Operation;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V1. 기본 캡처
 *
 * 통과 기준
 *   - INSERT/UPDATE/DELETE 가 목표 지연 안에 도착한다 (기본 5초)
 *   - UPDATE 에서 어떤 필드가 무엇에서 무엇으로 바뀌었는지 식별된다
 *   - 1만 건 배치에서도 밀리지 않는다 (처리량 측정)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("V1. 기본 캡처")
class V1BasicCaptureTest extends VerificationSupport {

    private static final String SLOT = "verify_v1_slot";
    /** 목표 지연. 이 값을 넘으면 실패로 본다. */
    private static final Duration LATENCY_BUDGET = Duration.ofSeconds(5);

    private static Path offsetFile;
    private static CaptureHarness harness;

    @BeforeAll
    static void startCapture() throws IOException {
        VerificationReport.section("V1. 기본 캡처");
        createRecordFixture();
        truncateRecords();
        dropSlotQuietly(SLOT);

        offsetFile = Files.createTempDirectory("v1-offset").resolve("offsets.dat");
        harness = new CaptureHarness(SLOT, RECORD_PUBLICATION, "public." + RECORD_TABLE,
                offsetFile, new Properties()).start();

        // 슬롯 생성과 스트리밍 진입까지 기다린다
        long deadline = System.currentTimeMillis() + 30_000;
        while (!slotExists(SLOT) && System.currentTimeMillis() < deadline) {
            sleep(200);
        }
        assertThat(slotExists(SLOT)).as("슬롯이 만들어져야 캡처가 시작된다").isTrue();
        sleep(1500); // 스트리밍 진입 직후의 첫 이벤트를 놓치지 않도록 여유
    }

    @AfterAll
    static void stopCapture() {
        if (harness != null) {
            harness.close();
        }
        dropSlotQuietly(SLOT);
    }

    @Test
    @Order(1)
    @DisplayName("INSERT 가 목표 지연 안에 도착한다")
    void insertIsCapturedWithinBudget() {
        harness.clearQueue();
        long sentAt = System.currentTimeMillis();
        insertRecord("V1-INS-1", new BigDecimal("15000.00"), "NEW");

        ChangeEvent event = harness.poll(LATENCY_BUDGET);

        assertThat(event).as("INSERT 이벤트가 %s 안에 도착해야 한다", LATENCY_BUDGET).isNotNull();
        assertThat(event.op()).isEqualTo(Operation.CREATE);
        assertThat(event.after().text("biz_key")).isEqualTo("V1-INS-1");
        assertThat(event.after().decimal("amount")).isEqualByComparingTo("15000.00");

        long e2e = System.currentTimeMillis() - sentAt;
        VerificationReport.metric("V1", "INSERT 왕복 지연(측정 기준)", e2e + " ms");

        // source.ts_ms 는 DB 서버 시계, System.currentTimeMillis() 는 이 프로세스의 시계다.
        // 둘이 어긋나 있으면 "커밋 기준 지연"이 음수로도 나온다 — 지연이 아니라 시계 편차다.
        // 운영의 cdc_end_to_end_lag_seconds 가 같은 방식으로 계산되므로 편차를 반드시 남긴다.
        Long dbNowMs = Db.scalarOnSource(
                "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint", Long.class);
        long skew = dbNowMs == null ? 0 : dbNowMs - System.currentTimeMillis();
        VerificationReport.metric("V1", "DB 서버와 측정 프로세스의 시계 편차", skew + " ms");
        VerificationReport.metric("V1", "INSERT 커밋 기준 지연(편차 보정)",
                (System.currentTimeMillis() + skew - event.sourceTsMs()) + " ms");

        if (Math.abs(skew) > 1000) {
            VerificationReport.note("V1",
                    "주의: 시계 편차가 " + skew + " ms 다. ts_ms 기반 지연 지표는 "
                            + "DB 와 Provider 의 시계가 동기화된 환경에서만 신뢰할 수 있다");
        }
    }

    @Test
    @Order(2)
    @DisplayName("UPDATE 에서 바뀐 필드를 값까지 식별할 수 있다")
    void updateExposesFieldLevelChange() {
        harness.clearQueue();
        insertRecord("V1-UPD-1", new BigDecimal("20000.00"), "NEW");
        assertThat(harness.poll(LATENCY_BUDGET)).as("선행 INSERT 이벤트").isNotNull();

        long sentAt = System.currentTimeMillis();
        Db.onSource("UPDATE verify_record SET status = 'CONFIRMED', amount = 22000.00 "
                + "WHERE biz_key = 'V1-UPD-1'");

        ChangeEvent event = harness.poll(LATENCY_BUDGET);

        assertThat(event).as("UPDATE 이벤트가 도착해야 한다").isNotNull();
        assertThat(event.op()).isEqualTo(Operation.UPDATE);

        // 핵심: before 가 비어 있으면 "무엇에서 무엇으로" 를 말할 수 없다
        assertThat(event.before()).as("REPLICA IDENTITY FULL 이면 before 가 채워진다").isNotNull();
        assertThat(event.before().text("status")).isEqualTo("NEW");
        assertThat(event.after().text("status")).isEqualTo("CONFIRMED");
        assertThat(event.before().decimal("amount")).isEqualByComparingTo("20000.00");
        assertThat(event.after().decimal("amount")).isEqualByComparingTo("22000.00");

        Set<String> changed = changedFields(event);
        assertThat(changed).as("바뀐 필드만 정확히 뽑힌다").contains("status", "amount");
        assertThat(changed).as("건드리지 않은 필드는 변경으로 잡히지 않는다").doesNotContain("biz_key", "id");

        VerificationReport.metric("V1", "UPDATE 지연", (System.currentTimeMillis() - sentAt) + " ms");
        VerificationReport.metric("V1", "식별된 변경 필드", changed.toString());
        VerificationReport.note("V1", "status: NEW -> CONFIRMED, amount: 20000.00 -> 22000.00 판독 성공");
    }

    @Test
    @Order(3)
    @DisplayName("DELETE 의 before 로 삭제된 행 전체를 알 수 있다")
    void deleteExposesBeforeImage() {
        harness.clearQueue();
        insertRecord("V1-DEL-1", new BigDecimal("30000.00"), "NEW");
        assertThat(harness.poll(LATENCY_BUDGET)).as("선행 INSERT 이벤트").isNotNull();

        long sentAt = System.currentTimeMillis();
        Db.onSource("DELETE FROM verify_record WHERE biz_key = 'V1-DEL-1'");

        ChangeEvent event = harness.poll(LATENCY_BUDGET);

        assertThat(event).as("DELETE 이벤트가 도착해야 한다").isNotNull();
        assertThat(event.op()).isEqualTo(Operation.DELETE);
        assertThat(event.after()).as("DELETE 에는 after 가 없다").isNull();
        assertThat(event.before().text("biz_key")).isEqualTo("V1-DEL-1");
        assertThat(event.before().decimal("amount")).isEqualByComparingTo("30000.00");

        VerificationReport.metric("V1", "DELETE 지연", (System.currentTimeMillis() - sentAt) + " ms");
    }

    @Test
    @Order(4)
    @DisplayName("1만 건 배치에서도 유실 없이 따라온다")
    void bulkChangeThroughput() {
        final int batchSize = 10_000;
        harness.clearQueue();

        long start = System.currentTimeMillis();
        insertRecordsInOneTransaction("V1-BULK", batchSize);
        long committedAt = System.currentTimeMillis();

        List<ChangeEvent> events = harness.collect(batchSize, Duration.ofMinutes(3));
        long finishedAt = System.currentTimeMillis();

        long drainMs = finishedAt - committedAt;
        long throughput = drainMs == 0 ? batchSize : batchSize * 1000L / drainMs;

        VerificationReport.metric("V1", "배치 건수", String.valueOf(batchSize));
        VerificationReport.metric("V1", "source 커밋 소요", (committedAt - start) + " ms");
        VerificationReport.metric("V1", "캡처 완료까지", drainMs + " ms");
        VerificationReport.metric("V1", "캡처 처리량", throughput + " events/s");
        VerificationReport.metric("V1", "수신 건수", events.size() + " / " + batchSize);

        assertThat(events).as("1만 건이 하나도 빠지지 않고 도착해야 한다").hasSize(batchSize);
        assertThat(events).allSatisfy(e ->
                assertThat(e.op()).isEqualTo(Operation.CREATE));

        // 순서 보존도 함께 확인한다 — LSN 은 단조 증가해야 한다
        long previous = -1;
        for (ChangeEvent e : events) {
            assertThat(e.lsn()).as("LSN 이 역전되면 순서 보장이 깨진 것이다").isGreaterThan(previous);
            previous = e.lsn();
        }
        VerificationReport.note("V1", "1만 건 LSN 단조 증가 확인 — WAL 순서 보존됨");
    }
}
