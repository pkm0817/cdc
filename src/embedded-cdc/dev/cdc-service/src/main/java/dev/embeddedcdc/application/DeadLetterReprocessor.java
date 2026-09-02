package dev.embeddedcdc.application;

import dev.embeddedcdc.domain.model.PendingDeadLetter;
import dev.embeddedcdc.domain.port.out.DeadLetterStore;
import dev.embeddedcdc.infrastructure.config.CdcSourceProperties;
import dev.embeddedcdc.infrastructure.config.DeadLetterProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 격리된 이벤트를 다시 반영한다.
 *
 * <b>PENDING 을 자동으로 집지 않는다.</b> 원인이 고쳐졌는지는 사람만 안다 —
 * 자동 재시도를 돌리면 고쳐지지 않은 독성 건이 영원히 재시도되며 잡음만 쌓인다.
 * 운영자가 원인을 고친 뒤 상태를 바꾸는 것이 곧 재처리 신청이다.
 *
 * <pre>
 * UPDATE cdc_dead_letter SET status = 'RETRY_REQUESTED' WHERE id = 1;
 * </pre>
 *
 * <b>재처리가 안전한 이유는 LSN 가드에 있다.</b> 격리된 뒤 같은 행에 더 새로운 변경이
 * 이미 반영됐다면, 오래된 LSN 을 든 이 이벤트는 갱신 행 수 0 으로 차단된다.
 * 즉 순서가 뒤바뀐 재처리가 최신 값을 덮어쓰지 않는다.
 *
 * 다만 그 가드는 computer 에만 있다 — car 는 조건 없는 UPSERT 라
 * 재처리가 더 새로운 값을 덮어쓸 수 있다. car 를 재처리할 때는 그 사이 변경이 없었는지
 * 확인해야 한다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(prefix = "cdc.dead-letter", name = "reprocess-enabled",
        havingValue = "true", matchIfMissing = true)
public class DeadLetterReprocessor {

    private final DeadLetterStore deadLetters;
    private final BatchApplier applier;
    private final CdcSourceProperties source;
    private final DeadLetterProperties props;

    @Scheduled(fixedDelayString = "${cdc.dead-letter.reprocess-interval-ms:30000}")
    public void reprocess() {
        List<PendingDeadLetter> claimed =
                deadLetters.claimForRetry(source.name(), props.reprocessBatchSize());
        if (claimed.isEmpty()) {
            return;
        }

        log.info("DLQ 재처리 시작 — {}건", claimed.size());
        int resolved = 0;
        int failed = 0;

        for (PendingDeadLetter pending : claimed) {
            try {
                // 체크포인트를 올리지 않는 경로다 — 지나간 LSN 을 다시 적용하는 것이므로
                // 진행 지점을 건드리면 안 된다.
                applier.applyOne(source.name(), pending.event());
                deadLetters.markResolved(pending.id());
                resolved++;
                log.info("재처리 성공 dlqId={} table={} op={} lsn={}",
                        pending.id(), pending.event().table(),
                        pending.event().op().code(), pending.event().lsn());
            } catch (Exception e) {
                deadLetters.markRetryFailed(pending.id(), e);
                failed++;
                log.warn("재처리 실패 dlqId={} table={} lsn={} 사유={} — PENDING 으로 되돌림",
                        pending.id(), pending.event().table(), pending.event().lsn(), e.toString());
            }
        }

        log.info("DLQ 재처리 완료 — 성공 {}건, 실패 {}건", resolved, failed);
    }
}
