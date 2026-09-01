package persistence

import (
	"context"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
)

// MemberRepository 는 grade 존재 여부를 확인하지 않는다 —
// 확인해도 할 수 있는 일이 실패시키기뿐이고, target 에 FK 가 없어 DB 도 막지 않는다.
// 어긋남은 대사(V6)가 잡는다.
//
// 충돌 판정 컬럼이 id 뿐이라는 점에 유의할 것. source 의 email 에는 UNIQUE 가 있지만
// target 에는 걸지 않았다 — 소프트 삭제로 행이 남아 있는 상태에서 같은 이메일이
// 새 id 로 다시 들어오면 UNIQUE 위반으로 적재가 막히기 때문이다.
type MemberRepository struct {
	store *Store
}

func NewMemberRepository(store *Store) *MemberRepository {
	return &MemberRepository{store: store}
}

var _ port.MemberRepository = (*MemberRepository)(nil)

const memberUpsertSQL = `
INSERT INTO member (id, email, name, grade_id, point, created_at, updated_at,
                    deleted, source_lsn, synced_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, false, $8, now())
ON CONFLICT (id) DO UPDATE SET
    email      = EXCLUDED.email,
    name       = EXCLUDED.name,
    grade_id   = EXCLUDED.grade_id,
    point      = EXCLUDED.point,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    deleted    = EXCLUDED.deleted,
    source_lsn = EXCLUDED.source_lsn,
    synced_at  = EXCLUDED.synced_at
WHERE EXCLUDED.source_lsn > member.source_lsn`

func (r *MemberRepository) UpsertIfNewer(ctx context.Context, member model.Member) (int64, error) {
	lsn, err := lsnToInt64(member.SourceLSN)
	if err != nil {
		return 0, err
	}
	tag, err := r.store.exec(ctx).Exec(ctx, memberUpsertSQL,
		member.ID, member.Email, member.Name, member.GradeID, member.Point,
		member.CreatedAt, member.UpdatedAt, lsn)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (r *MemberRepository) SoftDelete(ctx context.Context, id int64, lsn uint64) (int64, error) {
	return softDelete(ctx, r.store, "member", id, lsn)
}
