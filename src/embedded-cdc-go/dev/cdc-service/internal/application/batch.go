package application

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/embedded-cdc-go/cdc-service/internal/application/handler"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
)

// BatchApplier 는 적용의 트랜잭션 경계다.
//
// 배치 적용과 체크포인트 기록이 같은 트랜잭션이다.
// 이것이 이 타입의 존재 이유다 — 둘이 갈라져 있으면
// "적용은 됐는데 어디까지 했는지는 모르는" 창이 생긴다.
// 커밋되면 둘 다, 롤백되면 둘 다다.
type BatchApplier struct {
	handlers    map[model.SourceTable]handler.TableSyncHandler
	checkpoints port.CheckpointStore
	changeAudit port.ChangeAuditStore
	tx          port.TransactionRunner
	log         *slog.Logger
}

func NewBatchApplier(
	handlers []handler.TableSyncHandler,
	checkpoints port.CheckpointStore,
	changeAudit port.ChangeAuditStore,
	tx port.TransactionRunner,
	log *slog.Logger,
) (*BatchApplier, error) {
	registry := make(map[model.SourceTable]handler.TableSyncHandler, len(handlers))
	for _, h := range handlers {
		if _, exists := registry[h.Table()]; exists {
			return nil, fmt.Errorf(
				"한 테이블에 핸들러가 둘이다: %s — 어느 쪽이 이길지 알 수 없다", h.Table().Name())
		}
		registry[h.Table()] = h
	}
	return &BatchApplier{
		handlers: registry, checkpoints: checkpoints,
		changeAudit: changeAudit, tx: tx, log: log,
	}, nil
}

// ApplyAll 은 배치 전체를 한 트랜잭션으로 적용하고 진행 지점까지 기록한다.
// 하나라도 실패하면 전부 롤백된다.
func (a *BatchApplier) ApplyAll(ctx context.Context, pipeline string, batch []model.ChangeEvent) error {
	return a.tx.InTx(ctx, func(txCtx context.Context) error {
		var highestLSN uint64
		for _, event := range batch {
			if err := a.apply(txCtx, pipeline, event); err != nil {
				return err
			}
			if event.LSN > highestLSN {
				highestLSN = event.LSN
			}
		}
		if highestLSN > 0 {
			return a.checkpoints.Record(txCtx, pipeline, highestLSN)
		}
		return nil
	})
}

// ApplyOne 은 한 건만 적용한다. 배치가 실패했을 때 범인을 좁히는 용도다.
// 이 경로에서는 체크포인트를 기록하지 않는다 — 격리가 끝난 뒤 한 번에 남긴다.
func (a *BatchApplier) ApplyOne(ctx context.Context, pipeline string, event model.ChangeEvent) error {
	return a.tx.InTx(ctx, func(txCtx context.Context) error {
		return a.apply(txCtx, pipeline, event)
	})
}

func (a *BatchApplier) RecordCheckpoint(ctx context.Context, pipeline string, lsn uint64) error {
	return a.tx.InTx(ctx, func(txCtx context.Context) error {
		return a.checkpoints.Record(txCtx, pipeline, lsn)
	})
}

func (a *BatchApplier) apply(ctx context.Context, pipeline string, event model.ChangeEvent) error {
	table, known := model.SourceTableFromName(event.Table)
	if !known {
		// publication 이 바뀌어 모르는 테이블이 흘러 들어온 경우다.
		// 적용할 대상이 없으므로 실패가 아니라 무시다 — DLQ 로 보내면 잡음만 쌓인다.
		a.log.Warn("매핑되지 않은 테이블, 건너뜀", "table", event.Table)
		return nil
	}
	h, registered := a.handlers[table]
	if !registered {
		a.log.Warn("핸들러가 등록되지 않은 테이블, 건너뜀", "table", event.Table)
		return nil
	}
	if err := h.Apply(ctx, event); err != nil {
		return err
	}
	return a.auditIfUpdate(ctx, pipeline, event)
}

// auditIfUpdate 는 변경 이력을 UPDATE 만 남긴다.
//
// INSERT 는 모든 필드가 새 값이라 "바뀐 필드" 라는 개념이 성립하지 않고,
// DELETE 는 행 전체가 사라지는 것이라 필드 목록이 정보를 주지 않는다.
// 둘까지 남기면 대량 적재 때 이력 표가 원본 크기로 자라기만 한다.
//
// 같은 트랜잭션(호출자의 txCtx) 안에서 기록한다. 실패하면 적용도 함께 롤백되는데,
// 그것이 의도다 — "반영했다" 는 기록이 실제 반영과 갈라지면 감사 로그는 거짓말이 된다.
func (a *BatchApplier) auditIfUpdate(ctx context.Context, pipeline string, event model.ChangeEvent) error {
	if a.changeAudit == nil || event.Op != model.Update {
		return nil
	}
	diff := model.FieldDiffBetween(event.Before, event.After)
	if err := a.changeAudit.Record(ctx, pipeline, event, diff); err != nil {
		return fmt.Errorf("변경 이력 기록 실패: %w", err)
	}
	return nil
}
