package dev.embeddedcdc.domain.model;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * target 의 grade 한 행. member 가 참조하는 부모 테이블이다.
 *
 * source 와 컬럼이 같지만 sourceLsn 이 붙는다 — computer 와 같은 이유로
 * 순서 역전 방어가 필요하기 때문이다. deleted 와 syncedAt 이 없는 것도 computer 와 같다.
 */
public record Grade(
        long id,
        String code,
        String name,
        BigDecimal discountRate,
        OffsetDateTime createdAt,
        long sourceLsn
) {
}
