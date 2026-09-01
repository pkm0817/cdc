package handler

import (
	"log/slog"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/mapping"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
)

// NewMember 는 grade 를 참조하는 자식 테이블을 다룬다.
//
// grade 핸들러와 순서를 맞추려는 시도를 하지 않는다는 점이 중요하다.
// 한 배치 안에 grade INSERT 와 그것을 참조하는 member INSERT 가 같이 있어도
// BatchApplier 가 LSN 순서대로 부르므로 부모가 먼저 반영된다.
// target 에 FK 가 없으니 설령 순서가 어긋나도 적재는 성공하고, 어긋남은 대사가 잡는다.
func NewMember(repo port.MemberRepository, log *slog.Logger) TableSyncHandler {
	return &lsnGuarded[model.Member]{
		table:      model.TableMember,
		convert:    mapping.Member,
		upsert:     repo.UpsertIfNewer,
		softDelete: repo.SoftDelete,
		log:        log,
	}
}
