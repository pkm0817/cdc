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

    /** 감사 대상 테이블. 설정이 비어 있으면 빈 집합이고, 그때 감사 기록은 일어나지 않는다. */
    public Set<String> auditedTables() {
        if (auditChangedFields == null || auditChangedFields.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(auditChangedFields.split(","))
                .map(String::trim)
                .filter(table -> !table.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
