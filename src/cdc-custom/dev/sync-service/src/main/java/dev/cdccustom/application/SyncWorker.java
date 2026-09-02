package dev.cdccustom.application;

import dev.cdccustom.domain.PendingChanges;
import dev.cdccustom.infrastructure.config.SyncProperties;
import dev.cdccustom.infrastructure.jdbc.CheckpointStore;
import dev.cdccustom.infrastructure.jdbc.OutboxReader;
import dev.cdccustom.infrastructure.metrics.SyncMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

/**
 * 한 번의 동기화 주기. 이 방식의 전부가 여기 있다.
 *
 * <pre>
 *   1. outbox 를 seq 순으로 배치만큼 읽으며 (표, 행) 기준으로 접는다
 *   2. 접힌 목록을 타깃에 반영하고 체크포인트를 전진한다  ← BatchApplier (한 트랜잭션)
 *   3. 반영이 끝난 outbox 구간을 비운다                    ← 트랜잭션 밖
 * </pre>
 *
 * <p>CDC 와의 결정적 차이는 1번이다. CDC 이벤트는 값을 들고 오므로 접을 수 없고 온
 * 순서대로 전부 적용해야 한다. 여기서는 값을 반영 시점에 읽기 때문에 접을 수 있고,
 * 그래서 "같은 행을 여러 번 고치는" 부하에서는 해야 할 일 자체가 줄어든다.
 * 반대로 서로 다른 행만 한 번씩 바뀌면 접힐 것이 없어 이점도 없다.
 *
 * <p>대가는 분명하다 — 중간 상태가 남지 않으므로 변경 이력·감사에는 쓸 수 없다.
 */
@Service
public class SyncWorker {

    private static final Logger log = LoggerFactory.getLogger(SyncWorker.class);

    private final OutboxReader outbox;
    private final CheckpointStore checkpoint;
    private final BatchApplier applier;
    private final SyncProperties props;
    private final SyncMetrics metrics;

    private volatile long lastAppliedSeq;
    private volatile Instant lastRunAt = Instant.now();

    public SyncWorker(OutboxReader outbox,
                      CheckpointStore checkpoint,
                      BatchApplier applier,
                      SyncProperties props,
                      SyncMetrics metrics) {
        this.outbox = outbox;
        this.checkpoint = checkpoint;
        this.applier = applier;
        this.props = props;
        this.metrics = metrics;
    }

    /**
     * 한 주기를 돌고 이번에 반영한 행 수를 돌려준다.
     *
     * <p>0 이면 밀린 것이 없다는 뜻이다. 호출자는 그때만 쉰다 — 밀려 있는 동안 쉬면
     * 지연이 그만큼 늘어난다.
     */
    public int runOnce() {
        lastRunAt = Instant.now();

        long from = checkpoint.current();
        PendingChanges changes = outbox.readBatch(from, props.batchSize());
        if (changes.isEmpty()) {
            metrics.pending(0);
            return 0;
        }

        int applied = applier.apply(changes);

        // 접기 효과를 지표로 남긴다. 접힌 비율이 낮으면 이 방식의 이점이 없다는 뜻이므로
        // 판단 근거로 계속 보이게 둔다.
        metrics.batchApplied(changes.rawCount(), changes.foldedCount());
        Instant now = Instant.now();
        for (String table : changes.tables()) {
            metrics.lag(table, Duration.between(changes.oldestChange(table), now));
        }

        // 비우기는 트랜잭션 밖이다. 실패해도 다음 주기에 다시 지우면 되고, 남아 있어도
        // 체크포인트가 이미 앞서 있어 두 번 반영되지 않는다.
        outbox.prune(changes.maxSeq());
        lastAppliedSeq = changes.maxSeq();
        metrics.pending(outbox.pending(changes.maxSeq()));

        if (log.isDebugEnabled()) {
            log.debug("배치 반영 seq<={} 읽음={} 접힘={} 반영={}",
                    changes.maxSeq(), changes.rawCount(), changes.foldedCount(), applied);
        }
        return applied;
    }

    public long lastAppliedSeq() {
        return lastAppliedSeq;
    }

    public Instant lastRunAt() {
        return lastRunAt;
    }
}
