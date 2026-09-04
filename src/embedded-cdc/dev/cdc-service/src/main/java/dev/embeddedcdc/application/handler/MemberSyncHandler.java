package dev.embeddedcdc.application.handler;

import dev.embeddedcdc.domain.mapping.MemberMapper;
import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.SourceTable;
import dev.embeddedcdc.domain.port.out.MemberRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * member 이벤트를 target 에 반영한다.
 *
 * grade 핸들러와 순서를 맞추려는 시도를 하지 않는다는 점이 중요하다.
 * 한 배치 안에 grade INSERT 와 그것을 참조하는 member INSERT 가 같이 있어도
 * BatchApplier 가 LSN 순서대로 부르므로 부모가 먼저 반영된다.
 * target 에 FK 가 없으니 설령 순서가 어긋나도 적재는 성공하고, 어긋남은 대사가 잡는다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class MemberSyncHandler implements TableSyncHandler {

    private final MemberRepository repository;

    @Override
    public SourceTable table() {
        return SourceTable.MEMBER;
    }

    @Override
    public int apply(ChangeEvent event) {
        int affected = event.op().isUpsert()
                ? repository.upsertIfNewer(MemberMapper.from(event.after(), event.lsn()))
                : repository.softDelete(event.before().longValue("id"), event.lsn());

        if (affected == 0) {
            log.debug("더 오래된 이벤트라 차단됨 table=member op={} lsn={}", event.op().code(), event.lsn());
        }
        return affected;
    }
}
