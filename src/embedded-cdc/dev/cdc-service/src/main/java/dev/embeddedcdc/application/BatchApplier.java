package dev.embeddedcdc.application;

import dev.embeddedcdc.application.handler.TableSyncHandler;
import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.SourceTable;
import dev.embeddedcdc.domain.port.out.CheckpointStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * 적용의 트랜잭션 경계.
 *
 * <b>배치 적용과 체크포인트 기록이 같은 트랜잭션이다.</b>
 * 이것이 이 클래스의 존재 이유다 — 둘이 갈라져 있으면
 * "적용은 됐는데 어디까지 했는지는 모르는" 창이 생긴다.
 * 커밋되면 둘 다, 롤백되면 둘 다다.
 *
 * ChangeEventService 와 분리한 이유는 스프링 트랜잭션이 프록시 기반이라
 * 같은 빈 안에서 부르면 트랜잭션이 걸리지 않기 때문이다.
 */
@Component
@Slf4j
public class BatchApplier {

    private final Map<SourceTable, TableSyncHandler> handlers = new EnumMap<>(SourceTable.class);
    private final CheckpointStore checkpoints;

    public BatchApplier(List<TableSyncHandler> handlers, CheckpointStore checkpoints) {
        for (TableSyncHandler handler : handlers) {
            TableSyncHandler previous = this.handlers.put(handler.table(), handler);
            if (previous != null) {
                throw new IllegalStateException(
                        "한 테이블에 핸들러가 둘이다: " + handler.table() + " — 어느 쪽이 이길지 알 수 없다");
            }
        }
        this.checkpoints = checkpoints;
    }

    /** 배치 전체를 한 트랜잭션으로 적용하고 진행 지점까지 기록한다. 하나라도 실패하면 전부 롤백된다. */
    @Transactional
    public void applyAll(String pipeline, List<ChangeEvent> batch) {
        long highestLsn = 0;
        for (ChangeEvent event : batch) {
            apply(event);
            highestLsn = Math.max(highestLsn, event.lsn());
        }
        if (highestLsn > 0) {
            checkpoints.record(pipeline, highestLsn);
        }
    }

    /**
     * 한 건만 적용한다. 배치가 실패했을 때 범인을 좁히는 용도다.
     * 이 경로에서는 체크포인트를 기록하지 않는다 — 격리가 끝난 뒤 한 번에 남긴다.
     */
    @Transactional
    public void applyOne(ChangeEvent event) {
        apply(event);
    }

    @Transactional
    public void recordCheckpoint(String pipeline, long lsn) {
        checkpoints.record(pipeline, lsn);
    }

    private void apply(ChangeEvent event) {
        TableSyncHandler handler = SourceTable.fromName(event.table())
                .map(handlers::get)
                .orElse(null);

        if (handler == null) {
            // publication 이 바뀌어 모르는 테이블이 흘러 들어온 경우다.
            // 적용할 대상이 없으므로 실패가 아니라 무시다 — DLQ 로 보내면 잡음만 쌓인다.
            log.warn("매핑되지 않은 테이블, 건너뜀: {}", event.table());
            return;
        }
        handler.apply(event);
    }
}
