package dev.embeddedcdc.domain.model;

import java.math.BigDecimal;

/**
 * target 의 computer 한 행. source 와 컬럼이 전혀 다르다.
 *
 * deleted 와 synced_at 은 여기 없다 —
 * 전자는 삭제 경로에서만 세우고, 후자는 DB 가 now() 로 채우기 때문이다.
 */
public record Computer(
        long id,
        String fullName,
        String spec,
        BigDecimal priceKrw,
        long sourceLsn
) {
}
