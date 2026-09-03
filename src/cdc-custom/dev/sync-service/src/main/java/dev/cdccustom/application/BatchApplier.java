package dev.cdccustom.application;

import dev.cdccustom.domain.Op;
import dev.cdccustom.domain.PendingChanges;
import dev.cdccustom.domain.port.TableSyncer;
import dev.cdccustom.infrastructure.jdbc.CheckpointStore;
import dev.cdccustom.infrastructure.metrics.SyncMetrics;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 접힌 변경 목록을 타깃에 반영하고 진행 지점을 전진시킨다. <b>한 트랜잭션</b>이다.
 *
 * <p>{@link SyncWorker} 와 굳이 클래스를 나눈 이유는 스프링의 트랜잭션이 프록시로
 * 걸리기 때문이다. 같은 클래스 안에서 호출하면 프록시를 지나지 않아
 * {@code @Transactional} 이 통째로 무시된다 — 그러면 반영과 체크포인트가 따로 커밋돼
 * 이 설계의 핵심(둘 사이에 틈이 없다)이 조용히 깨진다.
 *
 * <p>{@code targetTransactionManager} 를 명시하는 것도 같은 이유다. DataSource 가 둘이라
 * 기본값에 맡기면 어느 쪽을 잡는지가 설정 순서에 따라 흔들린다.
 */
@Component
public class BatchApplier {

    private final Map<String, TableSyncer> syncers;
    private final CheckpointStore checkpoint;
    private final SyncMetrics metrics;

    public BatchApplier(List<TableSyncer> syncers, CheckpointStore checkpoint, SyncMetrics metrics) {
        this.syncers = syncers.stream().collect(Collectors.toMap(TableSyncer::table, Function.identity()));
        this.checkpoint = checkpoint;
        this.metrics = metrics;
    }

    /**
     * @return 타깃에 실제로 쓴 행 수
     */
    @Transactional(transactionManager = "targetTransactionManager")
    public int apply(PendingChanges changes) {
        int applied = 0;
        for (TableSyncer syncer : syncers.values()) {
            String table = syncer.table();

            // 삭제를 먼저 처리한다. 같은 배치 안에서 한 PK 는 접기 규칙상 삭제이거나
            // 갱신이거나 둘 중 하나뿐이라 순서가 결과를 바꾸지는 않지만,
            // 지운 뒤 새로 넣는 순서가 사람이 읽기에 자연스럽다.
            List<Long> deletes = changes.deleteIds(table);
            if (!deletes.isEmpty()) {
                applied += syncer.delete(deletes, changes.maxSeq());
                metrics.applied(table, "d", deletes.size());
            }

            List<Long> upserts = changes.upsertIds(table);
            if (!upserts.isEmpty()) {
                applied += syncer.upsert(upserts, changes.maxSeq());
                // 반영은 UPSERT 한 문장이지만 지표는 c 와 u 를 나눠 센다.
                // 전부 u 로 세면 INSERT 부하가 UPDATE 로 보인다 — CDC 두 스택과 라벨을
                // 맞춘 이유가 같은 눈금으로 비교하는 것인데, 그 눈금이 어긋난다.
                for (Op op : List.of(Op.CREATE, Op.UPDATE)) {
                    int n = changes.count(table, op);
                    if (n > 0) {
                        metrics.applied(table, op.code(), n);
                    }
                }
            }
        }
        checkpoint.advanceTo(changes.maxSeq());
        return applied;
    }
}
