package persistence

import (
	"context"
	"fmt"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
)

// CarRepository 는 source 와 스키마가 같아 변환 없이 그대로 저장한다.
//
// 조건절이 없다는 점에 주의 — 늦게 도착한 오래된 이벤트도 최신 값을 덮어쓴다.
// 단일 고루틴 순차 처리가 그 전제를 지탱하고 있다. 적재를 병렬화하는 순간
// computer 처럼 source_lsn 가드를 붙여야 한다.
type CarRepository struct {
	store *Store
}

func NewCarRepository(store *Store) *CarRepository {
	return &CarRepository{store: store}
}

var _ port.CarRepository = (*CarRepository)(nil)

const carUpsertSQL = `
INSERT INTO car (id, name, brand, price, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (id) DO UPDATE SET
    name       = EXCLUDED.name,
    brand      = EXCLUDED.brand,
    price      = EXCLUDED.price,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at`

func (r *CarRepository) Upsert(ctx context.Context, car model.Car) error {
	price, err := numeric(car.Price)
	if err != nil {
		return fmt.Errorf("%w: car.price: %v", model.ErrBadData, err)
	}
	_, err = r.store.exec(ctx).Exec(ctx, carUpsertSQL,
		car.ID, car.Name, car.Brand, price, car.CreatedAt, car.UpdatedAt)
	return err
}

func (r *CarRepository) Delete(ctx context.Context, id int64) error {
	_, err := r.store.exec(ctx).Exec(ctx, `DELETE FROM car WHERE id = $1`, id)
	return err
}
