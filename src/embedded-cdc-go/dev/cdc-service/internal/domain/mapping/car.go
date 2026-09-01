// Package mapping 은 source 의 행을 target 의 행으로 옮기는 순수 함수 모음이다.
//
// DB 도 라이브러리도 없이 단위 테스트할 수 있어야 한다는 것이 이 패키지의 규칙이다.
// 변환 규칙이 늘어나면 여기만 커진다.
package mapping

import "github.com/embedded-cdc-go/cdc-service/internal/domain/model"

// Car 는 source 의 car 행을 target 의 car 로 옮긴다.
//
// 변환 규칙이 없다는 것이 이 함수의 내용이다. 그래도 함수를 두는 이유는
// "source 컬럼 이름을 아는 곳"을 한 군데로 모으기 위해서다 —
// 컬럼명이 여기저기 흩어지면 source 스키마가 바뀔 때 어디를 고쳐야 하는지 알 수 없다.
func Car(row model.RowData) (model.Car, error) {
	id, err := row.Int64("id")
	if err != nil {
		return model.Car{}, err
	}
	name, err := row.Text("name")
	if err != nil {
		return model.Car{}, err
	}
	brand, err := row.Text("brand")
	if err != nil {
		return model.Car{}, err
	}
	price, err := row.Decimal("price")
	if err != nil {
		return model.Car{}, err
	}
	createdAt, err := row.Timestamp("created_at")
	if err != nil {
		return model.Car{}, err
	}
	updatedAt, err := row.Timestamp("updated_at")
	if err != nil {
		return model.Car{}, err
	}
	return model.Car{
		ID:        id,
		Name:      name,
		Brand:     brand,
		Price:     price,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}
