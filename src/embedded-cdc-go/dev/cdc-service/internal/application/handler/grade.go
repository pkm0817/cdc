package handler

import (
	"log/slog"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/mapping"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
)

// NewGrade 는 member 의 부모 테이블을 다룬다. computer 와 같은 형태다.
//
// 삭제가 소프트인 것이 여기서는 필수다 — member.grade_id 가 여전히 이 id 를 가리키고 있어서,
// 물리 삭제하면 남은 member 의 등급을 되짚을 수 없게 된다.
func NewGrade(repo port.GradeRepository, log *slog.Logger) TableSyncHandler {
	return &lsnGuarded[model.Grade]{
		table:      model.TableGrade,
		convert:    mapping.Grade,
		upsert:     repo.UpsertIfNewer,
		softDelete: repo.SoftDelete,
		log:        log,
	}
}
