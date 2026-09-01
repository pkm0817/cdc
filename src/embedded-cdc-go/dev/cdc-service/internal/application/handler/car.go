package handler

import (
	"context"
	"fmt"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/mapping"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
)

// Car 는 source 와 target 스키마가 같은 경우를 다룬다.
//
// 순서 역전 방어가 없다. 지금은 라이브러리가 단일 고루틴으로 WAL 순서를 그대로 전달하고
// 적재도 한 고루틴에서만 일어나므로 안전하지만, sink 를 병렬화하거나 인스턴스를 늘리는
// 순간 이 테이블이 먼저 깨진다. 그때는 ComputerRepository 처럼 저장소 계약에
// 순서 조건을 넣어야 한다.
type Car struct {
	repo port.CarRepository
}

func NewCar(repo port.CarRepository) *Car {
	return &Car{repo: repo}
}

func (h *Car) Table() model.SourceTable {
	return model.TableCar
}

func (h *Car) Apply(ctx context.Context, event model.ChangeEvent) error {
	if event.Op.IsUpsert() {
		if event.After == nil {
			return fmt.Errorf("%w: car %s 이벤트에 after 가 없다", model.ErrBadData, event.Op.Code())
		}
		car, err := mapping.Car(*event.After)
		if err != nil {
			return err
		}
		return h.repo.Upsert(ctx, car)
	}

	if event.Before == nil {
		return fmt.Errorf("%w: car 삭제 이벤트에 before 가 없다", model.ErrBadData)
	}
	id, err := event.Before.Int64("id")
	if err != nil {
		return err
	}
	return h.repo.Delete(ctx, id)
}
