package handler

import (
	"log/slog"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/mapping"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
)

// NewComputer 는 source 와 target 스키마가 다른 경우를 다룬다.
// 변환은 도메인 매퍼가 하고, 여기서는 "어떤 저장소 연산을 부를지"만 고른다.
func NewComputer(repo port.ComputerRepository, log *slog.Logger) TableSyncHandler {
	return &lsnGuarded[model.Computer]{
		table:      model.TableComputer,
		convert:    mapping.Computer,
		upsert:     repo.UpsertIfNewer,
		softDelete: repo.SoftDelete,
		log:        log,
	}
}
