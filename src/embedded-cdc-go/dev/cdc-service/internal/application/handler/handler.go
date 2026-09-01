// Package handler 는 테이블 하나를 target 에 반영하는 방법을 담는다.
package handler

import (
	"context"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
)

// TableSyncHandler 는 테이블 하나를 target 에 반영하는 방법이다.
//
// 테이블을 추가하려면 이 인터페이스 구현 하나만 등록하면 된다 —
// ChangeEventService 는 고칠 필요가 없다. switch 문을 두지 않은 이유다.
type TableSyncHandler interface {
	Table() model.SourceTable
	Apply(ctx context.Context, event model.ChangeEvent) error
}
