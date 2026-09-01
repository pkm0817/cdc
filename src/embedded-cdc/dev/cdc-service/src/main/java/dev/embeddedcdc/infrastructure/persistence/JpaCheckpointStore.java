package dev.embeddedcdc.infrastructure.persistence;

import dev.embeddedcdc.domain.port.out.CheckpointStore;
import dev.embeddedcdc.infrastructure.persistence.entity.CheckpointEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.OptionalLong;

/**
 * CheckpointStore 의 JPA 구현.
 *
 * 배치마다 한 번만 호출되므로 save() 의 merge(SELECT 후 UPDATE) 비용은 무시할 만하다.
 * 이벤트마다 부르면 왕복이 두 배가 되니 호출 지점을 늘리지 말 것.
 */
@Repository
@RequiredArgsConstructor
public class JpaCheckpointStore implements CheckpointStore {

    private final CheckpointJpaRepository jpa;

    @Override
    @Transactional(readOnly = true)
    public OptionalLong lastAppliedLsn(String pipeline) {
        return jpa.findById(pipeline)
                .map(CheckpointEntity::getLastAppliedLsn)
                .map(OptionalLong::of)
                .orElseGet(OptionalLong::empty);
    }

    @Override
    @Transactional
    public void record(String pipeline, long lsn) {
        jpa.save(new CheckpointEntity(pipeline, lsn, OffsetDateTime.now()));
    }
}
