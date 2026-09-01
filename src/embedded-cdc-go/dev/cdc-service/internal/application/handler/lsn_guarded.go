package handler

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
)

// lsnGuarded 는 computer·grade·member 가 공유하는 형태다 —
// LSN 가드가 붙은 멱등 UPSERT + 소프트 삭제.
//
// Java 판에서는 세 핸들러가 각각 같은 모양의 코드를 갖고 있었다. 여기서는 한곳에 모았다.
// 표마다 다른 것은 "어떤 매퍼로 바꾸고 어떤 저장소를 부르는가"뿐이고,
// 그 셋을 생성자에서 주입한다. 표별 사정은 각 생성자 주석에 남긴다.
//
// 반영 행 수가 0 이면 더 오래된 이벤트가 차단된 것이다. 오류가 아니라 정상 동작이므로
// 오류를 돌려주지 않고 debug 로만 남긴다 — error 로 올리면 대시보드가 거짓 경보를 낸다.
type lsnGuarded[T any] struct {
	table      model.SourceTable
	convert    func(model.RowData, uint64) (T, error)
	upsert     func(context.Context, T) (int64, error)
	softDelete func(context.Context, int64, uint64) (int64, error)
	log        *slog.Logger
}

func (h *lsnGuarded[T]) Table() model.SourceTable {
	return h.table
}

func (h *lsnGuarded[T]) Apply(ctx context.Context, event model.ChangeEvent) error {
	var (
		affected int64
		err      error
	)

	if event.Op.IsUpsert() {
		if event.After == nil {
			return fmt.Errorf("%w: %s %s 이벤트에 after 가 없다",
				model.ErrBadData, h.table.Name(), event.Op.Code())
		}
		var row T
		row, err = h.convert(*event.After, event.LSN)
		if err != nil {
			return err
		}
		affected, err = h.upsert(ctx, row)
	} else {
		if event.Before == nil {
			return fmt.Errorf("%w: %s 삭제 이벤트에 before 가 없다",
				model.ErrBadData, h.table.Name())
		}
		var id int64
		id, err = event.Before.Int64("id")
		if err != nil {
			return err
		}
		affected, err = h.softDelete(ctx, id, event.LSN)
	}

	if err != nil {
		return err
	}
	if affected == 0 {
		h.log.Debug("더 오래된 이벤트라 차단됨",
			"table", h.table.Name(), "op", event.Op.Code(), "lsn", event.LSN)
	}
	return nil
}
