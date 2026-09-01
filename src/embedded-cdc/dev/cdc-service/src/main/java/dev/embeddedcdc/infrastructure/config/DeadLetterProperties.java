package dev.embeddedcdc.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * DLQ 재처리 정책.
 *
 * @param reprocessEnabled    재처리 스케줄러 사용 여부
 * @param reprocessIntervalMs 재처리 주기(밀리초). RETRY_REQUESTED 가 없으면 아무 일도 하지 않는다
 * @param reprocessBatchSize  한 번에 집는 건수. 너무 크면 정상 파이프라인과 DB 를 다툰다
 */
@ConfigurationProperties(prefix = "cdc.dead-letter")
public record DeadLetterProperties(
        boolean reprocessEnabled,
        long reprocessIntervalMs,
        int reprocessBatchSize
) {
}
