package dev.embeddedcdc.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 적용 실패를 다루는 정책값.
 *
 * @param maxBatchRetries       배치 전체를 다시 시도하는 횟수. 소진하면 건 단위 격리로 넘어간다
 * @param retryBackoffMs        첫 재시도 대기(밀리초). 시도마다 두 배로 늘어난다
 * @param haltOnDeadLetterRatio 한 배치에서 이 비율을 넘게 격리되면 구조 문제로 보고 멈춘다.
 *                              0.5 면 절반이다. 낮출수록 보수적이다
 */
@ConfigurationProperties(prefix = "cdc.apply")
public record CdcApplyProperties(
        int maxBatchRetries,
        long retryBackoffMs,
        double haltOnDeadLetterRatio
) {
}
