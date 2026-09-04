package dev.embeddedcdc.application.handler;

import dev.embeddedcdc.domain.mapping.CarMapper;
import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.SourceTable;
import dev.embeddedcdc.domain.port.out.CarRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * car — source 와 target 스키마가 같은 경우.
 *
 * <b>순서 역전 방어가 저장소 계약에 들어 있다.</b> 예전에는 없었고 근거를 이렇게 적어 두었었다 —
 * "엔진이 단일 스레드로 WAL 순서를 그대로 전달하므로 안전하다". 그 전제가 틀렸다:
 * <b>DLQ 재처리는 그 단일 스레드 경로 밖</b>에서 돈다(DeadLetterReprocessor 가 별도 스케줄러로
 * applyOne 을 직접 부른다). 격리된 뒤 재처리까지의 사이에 정상 경로로 같은 행이 갱신되면
 * 오래된 이벤트가 최신 값을 덮는다 — V4-b 에서 V4-FIXED 가 V4-POISON 으로 되돌아갔다.
 * 병렬화나 인스턴스 증설을 기다릴 필요 없이 이미 깨지고 있었다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CarSyncHandler implements TableSyncHandler {

    private final CarRepository repository;

    @Override
    public SourceTable table() {
        return SourceTable.CAR;
    }

    @Override
    public int apply(ChangeEvent event) {
        int affected = event.op().isUpsert()
                ? repository.upsertIfNewer(CarMapper.from(event.after(), event.lsn()))
                : repository.deleteIfNewer(event.before().longValue("id"), event.lsn());

        if (affected == 0) {
            log.debug("더 오래된 이벤트라 차단됨 table=car op={} lsn={}", event.op().code(), event.lsn());
        }
        return affected;
    }
}
