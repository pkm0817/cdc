package dev.embeddedcdc.verification;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 검증 계측치 수집기.
 *
 * 테스트가 통과했는지(assert)와 별개로 "무엇이 얼마였는지"를 남긴다.
 * 검증 보고서의 값은 전부 여기서 나오며, 사람이 나중에 손으로 옮겨 적지 않는다.
 */
public final class VerificationReport {

    private static final List<String> LINES = new CopyOnWriteArrayList<>();
    private static final Path OUT = Path.of(
            System.getProperty("cdc.verify.report", "build/verification/results.md"));

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(VerificationReport::flush));
    }

    private VerificationReport() {
    }

    /** 항목 제목. 각 테스트 클래스가 시작할 때 한 번 부른다. */
    public static synchronized void section(String title) {
        String line = System.lineSeparator() + "## " + title;
        LINES.add(line);
        System.out.println("[검증] " + title);
    }

    /** 계측치 한 줄. 이름과 값, 그리고 판정 근거를 함께 남긴다. */
    public static synchronized void metric(String item, String name, String value) {
        LINES.add("- **" + item + "** · " + name + ": `" + value + "`");
        System.out.println("[" + item + "] " + name + " = " + value);
    }

    /** 관측된 사실(수치가 아닌 동작). 통과/실패와 무관하게 기록한다. */
    public static synchronized void note(String item, String observation) {
        LINES.add("- **" + item + "** · " + observation);
        System.out.println("[" + item + "] " + observation);
    }

    private static void flush() {
        try {
            Files.createDirectories(OUT.getParent());
            String body = "# CDC 캡처 신뢰성 검증 계측치" + System.lineSeparator()
                    + String.join(System.lineSeparator(), LINES) + System.lineSeparator();
            Files.writeString(OUT, body, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            System.out.println("[검증] 계측치 기록: " + OUT.toAbsolutePath());
        } catch (IOException e) {
            System.err.println("[검증] 계측치 기록 실패: " + e.getMessage());
        }
    }
}
