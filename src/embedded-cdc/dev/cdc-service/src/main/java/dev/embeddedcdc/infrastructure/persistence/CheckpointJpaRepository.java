package dev.embeddedcdc.infrastructure.persistence;

import dev.embeddedcdc.infrastructure.persistence.entity.CheckpointEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CheckpointJpaRepository extends JpaRepository<CheckpointEntity, String> {
}
