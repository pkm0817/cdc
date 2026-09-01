package mapping

import (
	"fmt"
	"math/big"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
)

// UsdToKrw 는 데모용 고정 환율이다.
//
// 실제 서비스라면 환율은 시점에 따라 달라지므로 상수가 아니라
// 별도 out 포트(예: ExchangeRateProvider)로 나가야 한다.
var UsdToKrw = big.NewRat(1350, 1)

// Computer 는 source 의 computer 행을 target 스키마로 변환한다.
// 이 파이프라인의 유일한 업무 규칙이다.
//
//	brand + model      -> full_name
//	cpu + ram_gb       -> spec
//	price_usd x 1350   -> price_krw
func Computer(row model.RowData, lsn uint64) (model.Computer, error) {
	id, err := row.Int64("id")
	if err != nil {
		return model.Computer{}, err
	}
	brand, err := row.Text("brand")
	if err != nil {
		return model.Computer{}, err
	}
	modelName, err := row.Text("model")
	if err != nil {
		return model.Computer{}, err
	}
	cpu, err := row.Text("cpu")
	if err != nil {
		return model.Computer{}, err
	}
	ramGB, err := row.Int32("ram_gb")
	if err != nil {
		return model.Computer{}, err
	}
	priceUSD, err := row.DecimalRat("price_usd")
	if err != nil {
		return model.Computer{}, err
	}

	return model.Computer{
		ID:        id,
		FullName:  brand + " " + modelName,
		Spec:      fmt.Sprintf("%s / %dGB", cpu, ramGB),
		PriceKRW:  roundHalfUpToInteger(new(big.Rat).Mul(priceUSD, UsdToKrw)),
		SourceLSN: lsn,
	}, nil
}

// roundHalfUpToInteger 는 소수점 이하를 반올림해 정수 문자열로 만든다.
//
// big.Rat 의 FloatString(0) 은 반올림을 하지만 half-away-from-zero 규칙이라
// 금액에서 흔히 쓰는 HALF_UP 과 양수 구간에서 결과가 같다. price_krw 는 음수가
// 될 수 없으므로 이대로 쓴다 — Java 판의 RoundingMode.HALF_UP 과 값이 일치한다.
func roundHalfUpToInteger(v *big.Rat) string {
	return v.FloatString(0)
}
