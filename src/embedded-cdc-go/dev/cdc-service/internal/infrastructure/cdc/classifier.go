package cdc

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SQLStateClassifier 는 PostgreSQL 오류코드로 실패의 성격을 가른다.
//
// 오류 문자열을 훑지 않고 코드로 판정하는 것이 핵심이다. 문자열 매칭은 로캘과
// 서버 버전에 따라 달라져, 어느 날 조용히 커넥션 장애가 격리 대상이 된다.
//
// 판정하지 못한 오류는 RETRY 로 본다. 한 번 더 시도해 보고 그래도 안 되면
// 건 단위 격리에서 어차피 DLQ 로 간다 — 모르는 것을 성급히 버리지 않기 위한 기본값이다.
type SQLStateClassifier struct {
	log *slog.Logger
}

func NewSQLStateClassifier(log *slog.Logger) *SQLStateClassifier {
	return &SQLStateClassifier{log: log}
}

var _ port.FailureClassifier = (*SQLStateClassifier)(nil)

// 시간이 지나면 저절로 풀린다.
var retryClasses = map[string]bool{"08": true, "53": true, "57": true}
var retryCodes = map[string]bool{"40001": true, "40P01": true, "55P03": true}

// 그 이벤트 하나의 데이터 문제다.
var deadLetterClasses = map[string]bool{"22": true, "23": true}

// 구조 문제다. 모든 이벤트가 같은 이유로 실패한다.
var haltClasses = map[string]bool{"28": true, "3D": true, "3F": true, "42": true}

func (c *SQLStateClassifier) Classify(cause error) model.FailureVerdict {
	if cause == nil {
		return model.VerdictRetry
	}

	var pgErr *pgconn.PgError
	if errors.As(cause, &pgErr) {
		if verdict, ok := bySQLState(pgErr.Code); ok {
			return verdict
		}
	}

	// 이벤트 값 자체를 읽지 못한 경우. 같은 값을 몇 번 넣어도 결과가 같으므로 격리 대상이다.
	if errors.Is(cause, model.ErrBadData) {
		return model.VerdictDeadLetter
	}

	// 커넥션이 끊겼거나 아직 못 붙은 경우. 격리하면 DB 재기동 30초 동안
	// 전체 트래픽이 DLQ 로 쏟아진다 — 그건 유실을 옮겨 담은 것이다.
	if isTransientConnection(cause) {
		return model.VerdictRetry
	}

	c.log.Debug("분류하지 못한 오류 — 우선 재시도로 본다", "error", cause)
	return model.VerdictRetry
}

func bySQLState(code string) (model.FailureVerdict, bool) {
	if len(code) < 2 {
		return model.VerdictRetry, false
	}
	if retryCodes[code] {
		return model.VerdictRetry, true
	}
	switch class := code[:2]; {
	case retryClasses[class]:
		return model.VerdictRetry, true
	case deadLetterClasses[class]:
		return model.VerdictDeadLetter, true
	case haltClasses[class]:
		return model.VerdictHalt, true
	}
	return model.VerdictRetry, false
}

func isTransientConnection(cause error) bool {
	var netErr net.Error
	switch {
	case errors.As(cause, &netErr):
		return true
	case errors.Is(cause, context.DeadlineExceeded):
		return true
	case errors.Is(cause, pgx.ErrTxClosed), errors.Is(cause, pgx.ErrTxCommitRollback):
		return true
	}
	// pgx 는 풀이 닫혔거나 커넥션을 얻지 못한 경우를 문자열 오류로 돌려준다.
	// 코드로 가릴 수 없는 유일한 구간이라 여기서만 문자열을 본다.
	message := cause.Error()
	return strings.Contains(message, "closed pool") ||
		strings.Contains(message, "conn closed") ||
		strings.Contains(message, "connection reset")
}
