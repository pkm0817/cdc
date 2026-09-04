package dev.embeddedcdc.infrastructure.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.querydsl.jpa.impl.JPAQueryFactory;
import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.Operation;
import dev.embeddedcdc.domain.model.PendingDeadLetter;
import dev.embeddedcdc.domain.model.RowData;
import dev.embeddedcdc.domain.port.out.DeadLetterStore;
import dev.embeddedcdc.infrastructure.persistence.entity.DeadLetterEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static dev.embeddedcdc.infrastructure.persistence.entity.QDeadLetterEntity.deadLetterEntity;

/**
 * DLQ 의 JPA 구현. 수신 측 DB 에 둔다.
 *
 * <b>왜 수신 측 DB 인가.</b> 별도 저장소에 두면 "적용도 실패, 격리 기록도 실패" 상황이 진짜 유실이 된다.
 * 같은 DB 에 두면 적어도 DB 가 살아 있는 한 기록은 남고, DB 자체가 죽었다면 그건 재시도 대상이라
 * 오프셋이 전진하지 않는다. 어느 쪽이든 유실 경로가 생기지 않는다.
 *
 * 기록에 REQUIRES_NEW 를 쓰는 이유는 격리 기록이 실패한 적용의 롤백에 휩쓸리면 안 되기 때문이다.
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class JpaDeadLetterStore implements DeadLetterStore {

    private static final int MESSAGE_LIMIT = 4000;

    private final DeadLetterJpaRepository jpa;
    private final JPAQueryFactory queryFactory;
    private final ObjectMapper mapper = new ObjectMapper();

    // ── 기록 ────────────────────────────────────────────────────────────────

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void store(String pipeline, ChangeEvent event, Throwable cause, int attempts) {
        jpa.save(new DeadLetterEntity(
                pipeline,
                event.table(),
                event.op().code(),
                event.lsn(),
                toPayload(event),
                cause.getClass().getName(),
                sqlStateOf(cause),
                truncate(String.valueOf(cause.getMessage())),
                attempts,
                OffsetDateTime.now()));
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void storeUnparsable(String pipeline, String rawPayload, Throwable cause) {
        jpa.save(new DeadLetterEntity(
                pipeline,
                "unknown",
                "unknown",
                0L,
                truncate(rawPayload),
                cause.getClass().getName(),
                sqlStateOf(cause),
                truncate(String.valueOf(cause.getMessage())),
                1,
                OffsetDateTime.now()));
    }

    @Override
    @Transactional(readOnly = true)
    public long pendingCount(String pipeline) {
        Long count = queryFactory
                .select(deadLetterEntity.count())
                .from(deadLetterEntity)
                .where(deadLetterEntity.pipeline.eq(pipeline)
                        .and(deadLetterEntity.status.in("PENDING", "RETRY_REQUESTED")))
                .fetchOne();
        return count == null ? 0 : count;
    }

    // ── 재처리 ──────────────────────────────────────────────────────────────

    @Override
    @Transactional
    public List<PendingDeadLetter> claimForRetry(String pipeline, int limit) {
        List<DeadLetterEntity> rows = queryFactory
                .selectFrom(deadLetterEntity)
                .where(deadLetterEntity.pipeline.eq(pipeline)
                        .and(deadLetterEntity.status.eq("RETRY_REQUESTED")))
                .orderBy(deadLetterEntity.id.asc())
                .limit(limit)
                .fetch();

        List<PendingDeadLetter> claimed = new ArrayList<>(rows.size());
        for (DeadLetterEntity row : rows) {
            ChangeEvent event = restore(row.getPayload());
            if (event == null) {
                // 복원조차 안 되면 재처리할 방법이 없다. 사람이 판단하도록 되돌린다.
                markStatus(row.getId(), "PENDING", "payload 복원 실패 — 수동 확인 필요");
                continue;
            }
            claimed.add(new PendingDeadLetter(row.getId(), event, row.getAttempts()));
        }
        return claimed;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markResolved(long id, String resolution) {
        queryFactory.update(deadLetterEntity)
                .set(deadLetterEntity.status, "RESOLVED")
                .set(deadLetterEntity.resolution, resolution)
                .set(deadLetterEntity.lastFailedAt, OffsetDateTime.now())
                .where(deadLetterEntity.id.eq(id))
                .execute();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markRetryFailed(long id, Throwable cause) {
        queryFactory.update(deadLetterEntity)
                .set(deadLetterEntity.status, "PENDING")
                .set(deadLetterEntity.attempts, deadLetterEntity.attempts.add(1))
                .set(deadLetterEntity.failureType, cause.getClass().getName())
                .set(deadLetterEntity.failureSqlState, sqlStateOf(cause))
                .set(deadLetterEntity.failureMessage, truncate(String.valueOf(cause.getMessage())))
                .set(deadLetterEntity.lastFailedAt, OffsetDateTime.now())
                .where(deadLetterEntity.id.eq(id))
                .execute();
    }

    private void markStatus(long id, String status, String message) {
        queryFactory.update(deadLetterEntity)
                .set(deadLetterEntity.status, status)
                .set(deadLetterEntity.failureMessage, truncate(message))
                .set(deadLetterEntity.lastFailedAt, OffsetDateTime.now())
                .where(deadLetterEntity.id.eq(id))
                .execute();
    }

    // ── payload 직렬화와 복원 ───────────────────────────────────────────────

    /**
     * 이벤트를 재구성 가능한 형태로 편다.
     * Debezium 원문이 아니라 도메인 값을 직렬화한다 — 이벤트 포맷이 바뀌어도 재처리가 깨지지 않는다.
     */
    private String toPayload(ChangeEvent event) {
        ObjectNode root = mapper.createObjectNode();
        root.put("table", event.table());
        root.put("op", event.op().code());
        root.put("lsn", event.lsn());
        root.put("sourceTsMs", event.sourceTsMs());
        root.set("before", toNode(event.before()));
        root.set("after", toNode(event.after()));
        try {
            return mapper.writeValueAsString(root);
        } catch (Exception e) {
            log.error("DLQ 페이로드 직렬화 실패 lsn={}", event.lsn(), e);
            return mapper.createObjectNode()
                    .put("table", event.table())
                    .put("lsn", event.lsn())
                    .put("serializationFailed", true)
                    .toString();
        }
    }

    /** toPayload 의 역방향. 복원이 되어야 재처리가 성립한다. */
    private ChangeEvent restore(String payload) {
        try {
            JsonNode root = mapper.readTree(payload);
            return Operation.fromCode(root.path("op").asText(null))
                    .map(op -> new ChangeEvent(
                            root.path("table").asText(),
                            op,
                            toRowData(root.get("before")),
                            toRowData(root.get("after")),
                            root.path("lsn").asLong(0L),
                            root.path("sourceTsMs").asLong(0L)))
                    .orElse(null);
        } catch (Exception e) {
            log.error("DLQ payload 복원 실패", e);
            return null;
        }
    }

    private JsonNode toNode(RowData row) {
        if (row == null) {
            return mapper.nullNode();
        }
        ObjectNode node = mapper.createObjectNode();
        row.values().forEach((k, v) -> {
            if (v == null) {
                node.putNull(k);
            } else {
                node.put(k, v);
            }
        });
        return node;
    }

    private RowData toRowData(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        Map<String, String> values = new HashMap<>();
        node.fields().forEachRemaining(f ->
                values.put(f.getKey(), f.getValue().isNull() ? null : f.getValue().asText()));
        return new RowData(values);
    }

    // ── 도우미 ──────────────────────────────────────────────────────────────

    private String sqlStateOf(Throwable cause) {
        for (Throwable t = cause; t != null; t = t.getCause()) {
            if (t instanceof SQLException sql) {
                return sql.getSQLState();
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return null;
    }

    private String truncate(String text) {
        if (text == null) {
            return "";
        }
        return text.length() <= MESSAGE_LIMIT ? text : text.substring(0, MESSAGE_LIMIT) + "...(잘림)";
    }
}
