package dev.embeddedcdc.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 적용 실패를 다루는 정책값.
 *
 * @param maxBatchRetries       배치 전체를 다시 시도하는 횟수. 소진하면 건 단위 격리로 넘어간다
 * @param retryBackoffMs        첫 재시도 대기(밀리초). 시도마다 두 배로 늘어난다
 * @param haltOnDeadLetterRatio 한 배치에서 이 비율을 넘게 격리되면 구조 문제로 보고 멈춘다.
 *                              0.5 면 절반이다. 낮출수록 보수적이다
 * @param auditChangedFields    UPDATE 의 변경 필드를 cdc_change_audit 에 남길 테이블(쉼표 구분).
 *                              비우면 끈다. 전역 on/off 가 아니라 목록인 이유는 비용 때문이다 —
 *                              이벤트당 INSERT 한 건이 더 붙어 1만 건 UPDATE 버스트에서
 *                              처리량이 약 3분의 1 줄고 최대 지연이 목표(5초)를 넘겼다(10.1초).
 *                              감사가 필요한 테이블에만 켜라는 뜻이다
 */
@ConfigurationProperties(prefix = "cdc.apply")
public record CdcApplyProperties(
        int maxBatchRetries,
        long retryBackoffMs,
        double haltOnDeadLetterRatio,
        String auditChangedFields
) {

    /**
     * 전체 테이블을 뜻하는 값. {@code CDC_AUDIT_CHANGED_FIELDS=*} 또는 {@code all}.
     * 목록에 섞여 있어도 전체로 본다 — "car,*" 는 "*" 다.
     */
    public static final String ALL = "*";

    /**
     * 감사 대상 테이블. 설정이 비어 있으면 빈 집합이고, 그때 감사 기록은 일어나지 않는다.
     * 전체({@link #ALL})가 지정된 경우 이 집합은 비어 있으므로, 판정은 {@link #audits(String)} 로 할 것.
     */
    public Set<String> auditedTables() {
        if (auditChangedFields == null || auditChangedFields.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(auditChangedFields.split(","))
                .map(CdcApplyProperties::normalize)
                .filter(table -> !table.isEmpty() && !ALL.equals(table))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** 전체 테이블에 감사를 켰는가. */
    public boolean auditsAllTables() {
        if (auditChangedFields == null || auditChangedFields.isBlank()) {
            return false;
        }
        return Arrays.stream(auditChangedFields.split(","))
                .map(CdcApplyProperties::normalize)
                .anyMatch(ALL::equals);
    }

    /**
     * 이 테이블의 UPDATE 를 감사 로그에 남기는가. 판정은 이 메서드 하나로 한다 —
     * 전체/일부/끔 세 경우를 호출자가 각각 따지게 두면 언젠가 하나를 빠뜨린다.
     */
    public boolean audits(String table) {
        return auditsAllTables() || auditedTables().contains(normalize(table));
    }

    /**
     * "public.car" 처럼 스키마가 붙어 와도 받아 준다 — Go 판 설정을 그대로 옮겨 오는 경우가 있다.
     * "all" 은 "*" 로 통일한다.
     */
    private static String normalize(String raw) {
        String s = raw == null ? "" : raw.trim();
        if (s.equalsIgnoreCase("all")) {
            return ALL;
        }
        int dot = s.lastIndexOf('.');
        return dot >= 0 ? s.substring(dot + 1) : s;
    }
}
