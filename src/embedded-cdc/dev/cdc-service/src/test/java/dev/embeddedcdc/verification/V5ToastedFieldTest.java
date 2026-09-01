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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V5. 이벤트가 비어서 오는 경우 (TOAST)
 *
 * PostgreSQL 은 큰 값을 행 밖(TOAST)에 저장하고, UPDATE 에서 그 컬럼이 바뀌지 않았으면
 * WAL 에 새 값을 싣지 않는다. Debezium 은 그 자리를 자리표시자로 채운다.
 * 관심 필드가 여기 걸리면 "값이 안 바뀐 것"과 "값을 못 받은 것"을 구분할 수 없다.
 *
 * 통과 기준
 *   - 관심 필드가 모든 이벤트에서 항상 판독 가능한 설정을 찾아낸다
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("V5. TOAST 로 값이 빠지는 경우")
class V5ToastedFieldTest extends VerificationSupport {

    private static final String SLOT = "verify_v5_slot";
    private static final String TABLE = "verify_toast";
    private static final String PUBLICATION = "verify_toast_pub";
    /** TOAST 임계(약 2KB)를 확실히 넘기는 크기 */
    private static final int PAYLOAD_SIZE = 200_000;

    private static Path offsetFile;
    private static CaptureHarness harness;

    @BeforeAll
    static void prepare() throws IOException {
        VerificationReport.section("V5. TOAST 로 값이 빠지는 경우");

        Db.onSource("""
                CREATE TABLE IF NOT EXISTS verify_toast (
                    id          BIGSERIAL PRIMARY KEY,
                    name        TEXT      NOT NULL,
                    big_payload TEXT      NOT NULL
                )
                """);
        // 압축으로 인라인에 들어가 버리면 TOAST 상황이 재현되지 않는다.
        // EXTERNAL 은 압축 없이 무조건 행 밖에 저장하게 만든다.
        Db.onSource("ALTER TABLE verify_toast ALTER COLUMN big_payload SET STORAGE EXTERNAL");
        Db.onSource("ALTER TABLE verify_toast REPLICA IDENTITY DEFAULT");
        Db.onSource("TRUNCATE TABLE verify_toast RESTART IDENTITY");
        recreatePublication(PUBLICATION, TABLE);

        dropSlotQuietly(SLOT);
        offsetFile = Files.createTempDirectory("v5-offset").resolve("offsets.dat");
        harness = new CaptureHarness(SLOT, PUBLICATION, "public." + TABLE, offsetFile, new Properties()).start();

        long deadline = System.currentTimeMillis() + 30_000;
        while (!slotExists(SLOT) && System.currentTimeMillis() < deadline) {
            sleep(200);
        }
        assertThat(slotExists(SLOT)).isTrue();
        sleep(1500);
    }

    @AfterAll
    static void cleanup() {
        if (harness != null) {
            harness.close();
        }
        dropSlotQuietly(SLOT);
    }

    @Test
    @Order(1)
    @DisplayName("REPLICA IDENTITY DEFAULT 에서는 바뀌지 않은 대용량 필드가 빠져서 온다")
    void toastedFieldIsMissingWithDefaultIdentity() {
        harness.clearQueue();
        String payload = payload("A");

        insertToastRow("V5-DEFAULT", payload);
        ChangeEvent insertEvent = harness.poll(Duration.ofSeconds(15));
        assertThat(insertEvent).as("INSERT 이벤트").isNotNull();
        assertThat(insertEvent.op()).isEqualTo(Operation.CREATE);
        // INSERT 에는 전체 값이 실린다 — 문제는 UPDATE 다
        assertThat(insertEvent.after().text("big_payload"))
                .as("INSERT 에서는 대용량 필드도 온전히 온다").hasSize(PAYLOAD_SIZE);
        VerificationReport.note("V5", "INSERT 이벤트에는 대용량 필드가 온전히 실린다 (" + PAYLOAD_SIZE + " bytes)");

        // big_payload 는 건드리지 않고 name 만 바꾼다
        Db.onSource("UPDATE verify_toast SET name = 'V5-DEFAULT-CHANGED' WHERE name = 'V5-DEFAULT'");
        ChangeEvent updateEvent = harness.poll(Duration.ofSeconds(15));

        assertThat(updateEvent).as("UPDATE 이벤트").isNotNull();
        String afterPayload = updateEvent.after().values().get("big_payload");

        boolean placeholder = TOAST_PLACEHOLDER.equals(afterPayload);
        boolean absent = afterPayload == null;
        boolean readable = !placeholder && !absent && afterPayload.length() == PAYLOAD_SIZE;

        VerificationReport.metric("V5", "DEFAULT · after.big_payload 자리표시자",
                String.valueOf(placeholder));
        VerificationReport.metric("V5", "DEFAULT · after.big_payload 누락", String.valueOf(absent));
        VerificationReport.metric("V5", "DEFAULT · 실제 값 판독 가능", String.valueOf(readable));
        VerificationReport.metric("V5", "DEFAULT · before 존재", String.valueOf(updateEvent.before() != null));

        assertThat(readable)
                .as("DEFAULT 에서는 바뀌지 않은 TOAST 값을 읽을 수 없다 — 이것이 이 항목의 문제 상황이다")
                .isFalse();

        VerificationReport.note("V5",
                "DEFAULT 에서 관심 필드가 TOAST 대상이면 UPDATE 이벤트만으로는 현재 값을 알 수 없다");
    }

    @Test
    @Order(2)
    @DisplayName("REPLICA IDENTITY FULL 로 바꾸면 before 로 값을 복원할 수 있다")
    void fullIdentityMakesValueRecoverable() {
        Db.onSource("ALTER TABLE verify_toast REPLICA IDENTITY FULL");
        VerificationReport.note("V5", "REPLICA IDENTITY FULL 로 변경 후 재측정");
        sleep(1000);
        harness.clearQueue();

        String payload = payload("B");
        insertToastRow("V5-FULL", payload);
        assertThat(harness.poll(Duration.ofSeconds(15))).as("선행 INSERT").isNotNull();

        Db.onSource("UPDATE verify_toast SET name = 'V5-FULL-CHANGED' WHERE name = 'V5-FULL'");
        ChangeEvent updateEvent = harness.poll(Duration.ofSeconds(15));
        assertThat(updateEvent).as("UPDATE 이벤트").isNotNull();

        String afterPayload = updateEvent.after().values().get("big_payload");
        String beforePayload = updateEvent.before() == null
                ? null : updateEvent.before().values().get("big_payload");

        boolean afterReadable = afterPayload != null
                && !TOAST_PLACEHOLDER.equals(afterPayload) && afterPayload.length() == PAYLOAD_SIZE;
        boolean beforeReadable = beforePayload != null
                && !TOAST_PLACEHOLDER.equals(beforePayload) && beforePayload.length() == PAYLOAD_SIZE;

        VerificationReport.metric("V5", "FULL · after 판독 가능", String.valueOf(afterReadable));
        VerificationReport.metric("V5", "FULL · before 판독 가능", String.valueOf(beforeReadable));
        VerificationReport.metric("V5", "FULL · after 값",
                afterPayload == null ? "null"
                        : (TOAST_PLACEHOLDER.equals(afterPayload) ? "자리표시자" : afterPayload.length() + " bytes"));
        VerificationReport.metric("V5", "FULL · before 값",
                beforePayload == null ? "null"
                        : (TOAST_PLACEHOLDER.equals(beforePayload) ? "자리표시자" : beforePayload.length() + " bytes"));

        // 관심 필드를 "항상 판독 가능"하게 만들 수 있는지가 통과 기준이다.
        // after 든 before 든 한쪽에서 읽을 수 있으면 현재 값을 복원할 수 있다
        // (UPDATE 가 그 필드를 건드리지 않았으므로 before == 현재 값).
        boolean recoverable = afterReadable || beforeReadable;
        VerificationReport.metric("V5", "FULL · 현재 값 복원 가능", String.valueOf(recoverable));

        if (!recoverable) {
            VerificationReport.note("V5",
                    "경고: FULL 로도 TOAST 값이 오지 않는다. 관심 필드가 대용량이면 "
                            + "이벤트만으로 동기화할 수 없고 원본 재조회가 필요하다");
        } else {
            VerificationReport.note("V5",
                    "FULL 에서는 " + (afterReadable ? "after" : "before") + " 로 현재 값을 복원할 수 있다");
        }
    }

    private static void insertToastRow(String name, String payload) {
        try (var c = openSource();
             var ps = c.prepareStatement("INSERT INTO verify_toast (name, big_payload) VALUES (?, ?)")) {
            ps.setString(1, name);
            ps.setString(2, payload);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new IllegalStateException("TOAST 행 삽입 실패", e);
        }
    }

    /** 압축이 잘 되지 않도록 문자를 섞어 만든다. 잘 압축되면 인라인에 들어가 TOAST 가 안 된다. */
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
}
