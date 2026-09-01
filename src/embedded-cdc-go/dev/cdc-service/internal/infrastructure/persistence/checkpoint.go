package persistence

import (
	"context"
	"errors"
	"time"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
	"github.com/jackc/pgx/v5"
)

// CheckpointRepository 는 진행 지점을 수신 측 DB 에 남긴다.
//
// 배치마다 한 번만 호출되므로 왕복 비용은 무시할 만하다.
// 이벤트마다 부르면 왕복이 두 배가 되니 호출 지점을 늘리지 말 것.
type CheckpointRepository struct {
	store *Store
}

func NewCheckpointRepository(store *Store) *CheckpointRepository {
	return &CheckpointRepository{store: store}
}

var _ port.CheckpointStore = (*CheckpointRepository)(nil)

func (r *CheckpointRepository) LastAppliedLSN(ctx context.Context, pipeline string) (uint64, bool, error) {
	var lsn int64
	err := r.store.exec(ctx).
		QueryRow(ctx, `SELECT last_applied_lsn FROM cdc_checkpoint WHERE pipeline = $1`, pipeline).
		Scan(&lsn)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return uint64(lsn), true, nil
}

// Record 는 진행 지점을 앞으로만 민다.
//
// 조건절이 있는 이유: DLQ 재처리 같은 경로에서 지나간 LSN 이 흘러 들어와도
// 진행 지점이 뒤로 가지 않아야 한다. 뒤로 가면 다음 기동에서 이미 반영한 구간을
// 다시 읽게 되고, 그건 유실은 아니지만 불필요한 중복이다.
func (r *CheckpointRepository) Record(ctx context.Context, pipeline string, lsn uint64) error {
	value, err := lsnToInt64(lsn)
	if err != nil {
		return err
	}
	_, err = r.store.exec(ctx).Exec(ctx, `
INSERT INTO cdc_checkpoint (pipeline, last_applied_lsn, updated_at)
VALUES ($1, $2, $3)
ON CONFLICT (pipeline) DO UPDATE SET
    last_applied_lsn = EXCLUDED.last_applied_lsn,
    updated_at       = EXCLUDED.updated_at
WHERE EXCLUDED.last_applied_lsn > cdc_checkpoint.last_applied_lsn`,
		pipeline, value, time.Now())
	return err
}
