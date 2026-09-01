package dev.embeddedcdc.domain.mapping;

import dev.embeddedcdc.domain.model.Computer;
import dev.embeddedcdc.domain.model.RowData;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * source 의 computer 행을 target 스키마로 변환한다. 이 파이프라인의 유일한 업무 규칙이다.
 *
 *   brand + model      -> full_name
 *   cpu + ram_gb       -> spec
 *   price_usd x 1350   -> price_krw
 *
 * 순수 함수라 DB 도 스프링도 없이 단위 테스트할 수 있다. 규칙이 늘어나면 여기만 커진다.
 */
public final class ComputerMapper {

    /**
     * 데모용 고정 환율. 실제 서비스라면 환율은 시점에 따라 달라지므로
     * 상수가 아니라 별도 out 포트(예: ExchangeRateProvider)로 나가야 한다.
     */
    public static final BigDecimal USD_TO_KRW = new BigDecimal("1350");

    private ComputerMapper() {
    }

    public static Computer from(RowData row, long lsn) {
        String fullName = row.text("brand") + " " + row.text("model");
        String spec = row.text("cpu") + " / " + row.intValue("ram_gb") + "GB";
        BigDecimal priceKrw = row.decimal("price_usd")
                .multiply(USD_TO_KRW)
                .setScale(0, RoundingMode.HALF_UP);

        return new Computer(row.longValue("id"), fullName, spec, priceKrw, lsn);
    }
}
