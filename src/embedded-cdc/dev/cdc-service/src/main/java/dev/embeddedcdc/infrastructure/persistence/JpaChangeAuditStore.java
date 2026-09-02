package dev.embeddedcdc.infrastructure.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.FieldDiff;
import dev.embeddedcdc.domain.model.RowData;
import dev.embeddedcdc.domain.port.out.ChangeAuditStore;
import dev.embeddedcdc.domain.port.out.PipelineMetrics;
import dev.embeddedcdc.infrastructure.config.CdcApplyProperties;
import dev.embeddedcdc.infrastructure.persistence.entity.ChangeAuditEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 변경 이력의 JPA 구현. 수신 측 DB 의 cdc_change_audit 에 남긴다.
 *
 * 트랜잭션을 새로 열지 않는다 — 부르는 쪽(BatchApplier)의 트랜잭션에 그대로 참여해
 * 적용이 롤백되면 이 기록도 같이 사라진다. 자세한 이유는 포트 주석에 있다.
 *
 * 대상은 테이블 목록으로 받는다(cdc.apply.audit-changed-fields). 전역 on/off 가 아닌 이유는
 * 비용이 작지 않기 때문이다 — 이벤트당 INSERT 한 건이 더 붙어 1만 건 UPDATE 버스트에서
 * 처리량이 약 3분의 1 줄었고 최대 지연이 목표 5초를 넘겼다(10.1초). 감사가 실제로 필요한
 * 테이블에만 켜야 하고, 목록이 비어 있으면 아무것도 기록하지 않는다.
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class JpaChangeAuditStore implements ChangeAuditStore {

    private final ChangeAuditJpaRepository jpa;
    private final PipelineMetrics metrics;
    private final CdcApplyProperties props;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void record(String pipeline, ChangeEvent event, FieldDiff diff) {
        if (!props.auditedTables().contains(event.table())) {
            return;
        }
        jpa.save(new ChangeAuditEntity(
                pipeline,
                event.table(),
                rowKeyOf(event),
                event.lsn(),
                sourceTimestamp(event),
                diff.identifiable(),
                String.join(",", diff.changed()),
                diff.unreadable().isEmpty() ? null : String.join(",", diff.unreadable()),
                subsetJson(event.before(), diff.changed()),
                subsetJson(event.after(), diff.changed())));

        // 지표는 건수까지만. 필드명을 레이블로 올리면 카디널리티가 터진다.
        metrics.changeAudited(event.table());
    }

    /**
     * 어느 행인지 짚기 위한 키. 이 파이프라인의 대상 테이블은 모두 단일 PK 인 id 를 쓴다.
     * 복합 키 테이블이 들어오면 여기서 조합해야 한다 — 그때까지는 없는 규칙을 만들지 않는다.
     */
    private String rowKeyOf(ChangeEvent event) {
        RowData source = event.after() != null ? event.after() : event.before();
        String id = source == null ? null : source.values().get("id");
        return id == null ? "?" : id;
    }

    private OffsetDateTime sourceTimestamp(ChangeEvent event) {
        long ts = event.sourceTsMs() > 0 ? event.sourceTsMs() : System.currentTimeMillis();
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneOffset.UTC);
    }

    /** 바뀐 필드만 골라 JSON 으로. 행 전체를 남기면 이 표가 원본보다 빨리 커진다. */
    private String subsetJson(RowData row, Set<String> columns) {
        if (row == null || columns.isEmpty()) {
            return null;
        }
        Map<String, String> subset = new LinkedHashMap<>();
        for (String column : columns) {
            subset.put(column, row.values().get(column));
        }
        try {
            return mapper.writeValueAsString(subset);
        } catch (Exception e) {
            // 이력 기록이 적용을 막아서는 안 된다. 값은 포기하고 필드명만 남긴다.
            log.warn("변경 이력 직렬화 실패 table={} lsn={}", row.values().keySet(), e.toString());
            return null;
        }
    }
}
