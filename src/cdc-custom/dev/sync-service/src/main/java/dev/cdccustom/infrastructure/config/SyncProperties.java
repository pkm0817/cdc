package dev.cdccustom.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 동기화 루프 설정.
 *
 * @param batchSize      한 주기에 읽을 outbox 줄 수. 이 방식에서 처리량을 올리는 유일한
 *                       손잡이다 — 크게 잡을수록 더 많이 접히고 왕복도 줄어든다.
 *                       대신 한 트랜잭션이 길어져 실패 시 되돌리는 양도 커진다.
 * @param pollIntervalMs 밀린 것이 없을 때만 쉬는 시간. 밀려 있는 동안은 쉬지 않는다.
 * @param clockSkewProbeIntervalMs source DB 와의 시계 편차를 재는 주기. 지연 지표를 믿어도 되는지
 *                       판정하는 값이라 부하와 무관하게 일정하게 돈다. CDC 두 스택과 같은 30초.
 *                       커넥션을 하나 잡았다 놓는 질의 한 번이라 이 주기로는 비용이 없다시피 하다.
 */
@ConfigurationProperties(prefix = "sync")
public record SyncProperties(int batchSize, long pollIntervalMs, long clockSkewProbeIntervalMs) {

    public SyncProperties {
        if (batchSize <= 0) {
            batchSize = 5_000;
        }
        if (pollIntervalMs <= 0) {
            pollIntervalMs = 200;
        }
        if (clockSkewProbeIntervalMs <= 0) {
            clockSkewProbeIntervalMs = 30_000;
        }
    }
}
