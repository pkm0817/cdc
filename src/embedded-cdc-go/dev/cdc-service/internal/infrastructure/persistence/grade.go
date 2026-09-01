package persistence

import (
	"context"
	"fmt"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
)

// GradeRepository 는 ComputerRepository 와 같은 형태다.
//
// 등급이 사라져도 행은 남긴다 — member.grade_id 가 여전히 이 id 를 가리키고 있어서,
// 물리 삭제하면 남은 member 의 등급을 되짚을 수 없게 된다.
type GradeRepository struct {
	store *Store
}

func NewGradeRepository(store *Store) *GradeRepository {
	return &GradeRepository{store: store}
}

var _ port.GradeRepository = (*GradeRepository)(nil)

const gradeUpsertSQL = `
INSERT INTO grade (id, code, name, discount_rate, created_at, deleted, source_lsn, synced_at)
VALUES ($1, $2, $3, $4, $5, false, $6, now())
ON CONFLICT (id) DO UPDATE SET
    code          = EXCLUDED.code,
    name          = EXCLUDED.name,
    discount_rate = EXCLUDED.discount_rate,
    created_at    = EXCLUDED.created_at,
    deleted       = EXCLUDED.deleted,
    source_lsn    = EXCLUDED.source_lsn,
    synced_at     = EXCLUDED.synced_at
WHERE EXCLUDED.source_lsn > grade.source_lsn`

func (r *GradeRepository) UpsertIfNewer(ctx context.Context, grade model.Grade) (int64, error) {
	rate, err := numeric(grade.DiscountRate)
	if err != nil {
		return 0, fmt.Errorf("%w: grade.discount_rate: %v", model.ErrBadData, err)
	}
	lsn, err := lsnToInt64(grade.SourceLSN)
	if err != nil {
		return 0, err
	}
	tag, err := r.store.exec(ctx).Exec(ctx, gradeUpsertSQL,
		grade.ID, grade.Code, grade.Name, rate, grade.CreatedAt, lsn)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (r *GradeRepository) SoftDelete(ctx context.Context, id int64, lsn uint64) (int64, error) {
	return softDelete(ctx, r.store, "grade", id, lsn)
}
