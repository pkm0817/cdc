package dev.embeddedcdc.application;

import dev.embeddedcdc.application.handler.TableSyncHandler;
import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.FieldDiff;
import dev.embeddedcdc.domain.model.Operation;
import dev.embeddedcdc.domain.model.SourceTable;
import dev.embeddedcdc.domain.port.out.ChangeAuditStore;
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
 *
 * 변경 이력(감사 로그)도 여기서 남긴다. 같은 트랜잭션이어야
 * "이력에는 있는데 target 에는 반영되지 않은" 행이 생기지 않는다.
 */
@Component
@Slf4j
public class BatchApplier {

    private final Map<SourceTable, TableSyncHandler> handlers = new EnumMap<>(SourceTable.class);
    private final CheckpointStore checkpoints;
    private final ChangeAuditStore changeAudit;

    public BatchApplier(List<TableSyncHandler> handlers, CheckpointStore checkpoints,
                        ChangeAuditStore changeAudit) {
        for (TableSyncHandler handler : handlers) {
            TableSyncHandler previous = this.handlers.put(handler.table(), handler);
            if (previous != null) {
                throw new IllegalStateException(
                        "한 테이블에 핸들러가 둘이다: " + handler.table() + " — 어느 쪽이 이길지 알 수 없다");
            }
        }
        this.checkpoints = checkpoints;
        this.changeAudit = changeAudit;
    }

    /** 배치 전체를 한 트랜잭션으로 적용하고 진행 지점까지 기록한다. 하나라도 실패하면 전부 롤백된다. */
    @Transactional
    public void applyAll(String pipeline, List<ChangeEvent> batch) {
        long highestLsn = 0;
        for (ChangeEvent event : batch) {
            apply(pipeline, event);
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
    public int applyOne(String pipeline, ChangeEvent event) {
        return apply(pipeline, event);
    }

    @Transactional
    public void recordCheckpoint(String pipeline, long lsn) {
        checkpoints.record(pipeline, lsn);
    }

    private int apply(String pipeline, ChangeEvent event) {
        TableSyncHandler handler = SourceTable.fromName(event.table())
                .map(handlers::get)
                .orElse(null);

        if (handler == null) {
            // publication 이 바뀌어 모르는 테이블이 흘러 들어온 경우다.
            // 적용할 대상이 없으므로 실패가 아니라 무시다 — DLQ 로 보내면 잡음만 쌓인다.
            //
            // 파이프라인 자신의 살림살이 테이블(cdc_heartbeat)은 여기로 오는 것이 정상이라
            // 경고를 내지 않는다. 그 이벤트가 여기까지 오는 이유는 applyAll 이 배치의
            // 최대 LSN 을 체크포인트로 남기기 때문이다 — 유휴 구간에 슬롯만 전진하고
            // 체크포인트가 멈춰 있으면 다음 기동에서 SlotContinuityGuard 가 그 간격을
            // 캡처 갭으로 읽는다. 실제로 그렇게 오탐이 났었다.
            if (isPipelineOwnTable(event.table())) {
                log.debug("파이프라인 자체 테이블, 적용 대상 아님(체크포인트만 전진): {}", event.table());
            } else {
                log.warn("매핑되지 않은 테이블, 건너뜀: {}", event.table());
            }
            return 0;
        }
        int affected = handler.apply(event);
        auditIfUpdate(pipeline, event);
        return affected;
    }

    /** cdc_ 로 시작하는 것은 파이프라인이 자기 목적으로 쓰는 표다 (heartbeat 등). */
    private boolean isPipelineOwnTable(String table) {
        return table != null && table.startsWith("cdc_");
    }

    /**
     * 변경 이력은 UPDATE 만 남긴다.
     *
     * INSERT 는 모든 필드가 새 값이라 "바뀐 필드"라는 개념이 성립하지 않고,
     * DELETE 는 행 전체가 사라지는 것이라 필드 목록이 정보를 주지 않는다.
     * 둘까지 남기면 대량 적재 때 이력 표가 원본 크기로 자라기만 한다.
     */
    private void auditIfUpdate(String pipeline, ChangeEvent event) {
        if (event.op() != Operation.UPDATE) {
            return;
        }
        FieldDiff diff = FieldDiff.between(event.before(), event.after());
        changeAudit.record(pipeline, event, diff);
    }
}
