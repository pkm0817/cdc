package dev.embeddedcdc.infrastructure.persistence;

import dev.embeddedcdc.infrastructure.persistence.entity.ChangeAuditEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ChangeAuditJpaRepository extends JpaRepository<ChangeAuditEntity, Long> {
}
