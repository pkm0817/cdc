package dev.embeddedcdc.application.handler;

import dev.embeddedcdc.domain.mapping.ComputerMapper;
import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.SourceTable;
import dev.embeddedcdc.domain.port.out.ComputerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * computer — source 와 target 스키마가 다른 경우. 변환은 도메인 매퍼가 하고
 * 여기서는 "어떤 저장소 연산을 부를지"만 고른다.
 *
 * 반영 행 수가 0 이면 더 오래된 이벤트가 차단된 것이다. 오류가 아니라 정상 동작이므로
 * 예외를 던지지 않고 debug 로만 남긴다 — error 로 올리면 대시보드가 거짓 경보를 낸다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ComputerSyncHandler implements TableSyncHandler {

    private final ComputerRepository repository;

    @Override
    public SourceTable table() {
        return SourceTable.COMPUTER;
    }

    @Override
    public void apply(ChangeEvent event) {
        int affected = event.op().isUpsert()
                ? repository.upsertIfNewer(ComputerMapper.from(event.after(), event.lsn()))
                : repository.softDelete(event.before().longValue("id"), event.lsn());

        if (affected == 0) {
            log.debug("더 오래된 이벤트라 차단됨 table=computer op={} lsn={}", event.op().code(), event.lsn());
        }
    }
}
