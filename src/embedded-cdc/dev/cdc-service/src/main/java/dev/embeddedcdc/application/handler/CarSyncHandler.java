package dev.embeddedcdc.application.handler;

import dev.embeddedcdc.domain.mapping.CarMapper;
import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.SourceTable;
import dev.embeddedcdc.domain.port.out.CarRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * car — source 와 target 스키마가 같은 경우.
 *
 * 순서 역전 방어가 없다. 지금은 엔진이 단일 스레드로 WAL 순서를 그대로 전달하므로 안전하지만,
 * sink 를 병렬화하거나 인스턴스를 늘리는 순간 이 테이블이 먼저 깨진다.
 * 그때는 ComputerRepository 처럼 저장소 계약에 순서 조건을 넣어야 한다.
 */
@Component
@RequiredArgsConstructor
public class CarSyncHandler implements TableSyncHandler {

    private final CarRepository repository;

    @Override
    public SourceTable table() {
        return SourceTable.CAR;
    }

    @Override
    public void apply(ChangeEvent event) {
        if (event.op().isUpsert()) {
            repository.upsert(CarMapper.from(event.after()));
        } else {
            repository.delete(event.before().longValue("id"));
        }
    }
}
