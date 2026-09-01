package dev.embeddedcdc.application.handler;

import dev.embeddedcdc.domain.mapping.GradeMapper;
import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.SourceTable;
import dev.embeddedcdc.domain.port.out.GradeRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * grade 이벤트를 target 에 반영한다. computer 와 같은 형태다 —
 * LSN 가드가 붙은 멱등 UPSERT + 소프트 삭제.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class GradeSyncHandler implements TableSyncHandler {

    private final GradeRepository repository;

    @Override
    public SourceTable table() {
        return SourceTable.GRADE;
    }

    @Override
    public void apply(ChangeEvent event) {
        int affected = event.op().isUpsert()
                ? repository.upsertIfNewer(GradeMapper.from(event.after(), event.lsn()))
                : repository.softDelete(event.before().longValue("id"), event.lsn());

        if (affected == 0) {
            log.debug("더 오래된 이벤트라 차단됨 table=grade op={} lsn={}", event.op().code(), event.lsn());
        }
    }
}
