package persistence

import (
	"context"
	"fmt"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
)

// ComputerRepository 는 두 연산 모두 "더 새로운 이벤트일 때만" 반영하며,
// 그 판정이 한 문장 안에서 끝난다.
//
// 판정을 한 문장 안에 두는 것이 핵심이다. "조회해서 비교한 뒤 갱신"으로 벌어지면
// 그 사이에 다른 이벤트가 끼어들 수 있다. ON CONFLICT ... WHERE 는 그 창을 없앤다.
//
// 반영 행 수 0 은 오류가 아니라 더 오래된 이벤트가 차단된 것이다.
type ComputerRepository struct {
	store *Store
}

func NewComputerRepository(store *Store) *ComputerRepository {
	return &ComputerRepository{store: store}
}

var _ port.ComputerRepository = (*ComputerRepository)(nil)

const computerUpsertSQL = `
INSERT INTO computer (id, full_name, spec, price_krw, deleted, source_lsn, synced_at)
VALUES ($1, $2, $3, $4, false, $5, now())
ON CONFLICT (id) DO UPDATE SET
    full_name  = EXCLUDED.full_name,
    spec       = EXCLUDED.spec,
    price_krw  = EXCLUDED.price_krw,
    deleted    = EXCLUDED.deleted,
    source_lsn = EXCLUDED.source_lsn,
    synced_at  = EXCLUDED.synced_at
WHERE EXCLUDED.source_lsn > computer.source_lsn`

func (r *ComputerRepository) UpsertIfNewer(ctx context.Context, computer model.Computer) (int64, error) {
	price, err := numeric(computer.PriceKRW)
	if err != nil {
		return 0, fmt.Errorf("%w: computer.price_krw: %v", model.ErrBadData, err)
	}
	lsn, err := lsnToInt64(computer.SourceLSN)
	if err != nil {
		return 0, err
	}
	tag, err := r.store.exec(ctx).Exec(ctx, computerUpsertSQL,
		computer.ID, computer.FullName, computer.Spec, price, lsn)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// SoftDelete 는 물리 삭제가 아니라 플래그만 세운다.
// 물리 삭제하면 늦게 도착한 UPDATE 가 행을 되살려 유령 데이터가 남는다.
//
// synced_at 은 프로세스 시각이 아니라 DB 의 now() 를 쓴다 —
// 여러 인스턴스가 붙어도 시각 기준이 하나로 유지되어야 지연 관측이 신뢰할 수 있다.
func (r *ComputerRepository) SoftDelete(ctx context.Context, id int64, lsn uint64) (int64, error) {
	return softDelete(ctx, r.store, "computer", id, lsn)
}
