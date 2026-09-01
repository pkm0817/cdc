package mapping

import "github.com/embedded-cdc-go/cdc-service/internal/domain/model"

// Grade 는 source 의 grade 행을 target 의 grade 로 옮긴다. 변환 규칙은 없다.
//
// Car 와 같은 이유로 존재한다 — source 컬럼 이름을 아는 곳을 한 군데로 모은다.
func Grade(row model.RowData, lsn uint64) (model.Grade, error) {
	id, err := row.Int64("id")
	if err != nil {
		return model.Grade{}, err
	}
	code, err := row.Text("code")
	if err != nil {
		return model.Grade{}, err
	}
	name, err := row.Text("name")
	if err != nil {
		return model.Grade{}, err
	}
	discountRate, err := row.Decimal("discount_rate")
	if err != nil {
		return model.Grade{}, err
	}
	createdAt, err := row.Timestamp("created_at")
	if err != nil {
		return model.Grade{}, err
	}
	return model.Grade{
		ID:           id,
		Code:         code,
		Name:         name,
		DiscountRate: discountRate,
		CreatedAt:    createdAt,
		SourceLSN:    lsn,
	}, nil
}
