package dev.embeddedcdc.infrastructure.persistence;

import dev.embeddedcdc.infrastructure.persistence.entity.DeadLetterEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DeadLetterJpaRepository extends JpaRepository<DeadLetterEntity, Long> {
}
