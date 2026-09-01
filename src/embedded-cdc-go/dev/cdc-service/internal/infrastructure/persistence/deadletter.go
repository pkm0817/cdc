package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
	"github.com/jackc/pgx/v5/pgconn"
)

const messageLimit = 4000

// DeadLetterRepository 는 DLQ 를 수신 측 DB 에 둔다.
//
// 왜 수신 측 DB 인가. 별도 저장소에 두면 "적용도 실패, 격리 기록도 실패" 상황이
// 진짜 유실이 된다. 같은 DB 에 두면 적어도 DB 가 살아 있는 한 기록은 남고,
// DB 자체가 죽었다면 그건 재시도 대상이라 ack 가 나가지 않는다.
// 어느 쪽이든 유실 경로가 생기지 않는다.
//
// 기록이 트랜잭션 밖에서 일어나는 것이 중요하다 — 격리 기록이 실패한 적용의
// 롤백에 휩쓸리면 안 된다. 호출자가 트랜잭션이 실리지 않은 ctx 로 부르는 것이
// Java 판의 REQUIRES_NEW 자리다.
type DeadLetterRepository struct {
	store *Store
	log   *slog.Logger
}

func NewDeadLetterRepository(store *Store, log *slog.Logger) *DeadLetterRepository {
	return &DeadLetterRepository{store: store, log: log}
}

var _ port.DeadLetterStore = (*DeadLetterRepository)(nil)

// ── 기록 ────────────────────────────────────────────────────────────────────

const insertDeadLetterSQL = `
INSERT INTO cdc_dead_letter (
    pipeline, source_table, op, source_lsn, payload,
    failure_type, failure_sql_state, failure_message, attempts,
    status, first_failed_at, last_failed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'PENDING', $10, $10)`

func (r *DeadLetterRepository) Store(
	ctx context.Context, pipeline string, event model.ChangeEvent, cause error, attempts int,
) error {
	lsn, err := lsnToInt64(event.LSN)
	if err != nil {
		// LSN 을 담을 수 없어도 기록 자체는 남겨야 한다. 0 으로 두고 사유를 메시지에 남긴다.
		lsn = 0
	}
	now := time.Now()
	_, err = r.store.exec(ctx).Exec(ctx, insertDeadLetterSQL,
		pipeline, event.Table, event.Op.Code(), lsn, r.toPayload(event),
		failureType(cause), sqlStateOf(cause), truncate(cause.Error()), attempts, now)
	return err
}

func (r *DeadLetterRepository) StoreUnparsable(
	ctx context.Context, pipeline, rawPayload string, cause error,
) error {
	now := time.Now()
	_, err := r.store.exec(ctx).Exec(ctx, insertDeadLetterSQL,
		pipeline, "unknown", "unknown", int64(0), truncate(rawPayload),
		failureType(cause), sqlStateOf(cause), truncate(cause.Error()), 1, now)
	return err
}

func (r *DeadLetterRepository) PendingCount(ctx context.Context, pipeline string) (int64, error) {
	var count int64
	err := r.store.exec(ctx).QueryRow(ctx, `
SELECT count(*) FROM cdc_dead_letter
 WHERE pipeline = $1 AND status IN ('PENDING', 'RETRY_REQUESTED')`, pipeline).Scan(&count)
	return count, err
}

// ── 재처리 ──────────────────────────────────────────────────────────────────

func (r *DeadLetterRepository) ClaimForRetry(
	ctx context.Context, pipeline string, limit int,
) ([]model.PendingDeadLetter, error) {
	rows, err := r.store.exec(ctx).Query(ctx, `
SELECT id, payload, attempts
  FROM cdc_dead_letter
 WHERE pipeline = $1 AND status = 'RETRY_REQUESTED'
 ORDER BY id
 LIMIT $2`, pipeline, limit)
	if err != nil {
		return nil, err
	}

	type row struct {
		id       int64
		payload  string
		attempts int
	}
	var scanned []row
	for rows.Next() {
		var rec row
		if err := rows.Scan(&rec.id, &rec.payload, &rec.attempts); err != nil {
			rows.Close()
			return nil, err
		}
		scanned = append(scanned, rec)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	claimed := make([]model.PendingDeadLetter, 0, len(scanned))
	for _, rec := range scanned {
		event, restoreErr := r.restore(rec.payload)
		if restoreErr != nil {
			// 복원조차 안 되면 재처리할 방법이 없다. 사람이 판단하도록 되돌린다.
			r.markStatus(ctx, rec.id, "PENDING", "payload 복원 실패 — 수동 확인 필요: "+restoreErr.Error())
			continue
		}
		claimed = append(claimed, model.PendingDeadLetter{
			ID: rec.id, Event: event, Attempts: rec.attempts,
		})
	}
	return claimed, nil
}

func (r *DeadLetterRepository) MarkResolved(ctx context.Context, id int64) error {
	_, err := r.store.exec(ctx).Exec(ctx, `
UPDATE cdc_dead_letter SET status = 'RESOLVED', last_failed_at = $2 WHERE id = $1`,
		id, time.Now())
	return err
}

func (r *DeadLetterRepository) MarkRetryFailed(ctx context.Context, id int64, cause error) error {
	_, err := r.store.exec(ctx).Exec(ctx, `
UPDATE cdc_dead_letter
   SET status            = 'PENDING',
       attempts          = attempts + 1,
       failure_type      = $2,
       failure_sql_state = $3,
       failure_message   = $4,
       last_failed_at    = $5
 WHERE id = $1`,
		id, failureType(cause), sqlStateOf(cause), truncate(cause.Error()), time.Now())
	return err
}

func (r *DeadLetterRepository) markStatus(ctx context.Context, id int64, status, message string) {
	_, err := r.store.exec(ctx).Exec(ctx, `
UPDATE cdc_dead_letter SET status = $2, failure_message = $3, last_failed_at = $4 WHERE id = $1`,
		id, status, truncate(message), time.Now())
	if err != nil {
		r.log.Warn("DLQ 상태 갱신 실패", "dlqId", id, "status", status, "error", err)
	}
}

// ── payload 직렬화와 복원 ───────────────────────────────────────────────────

// dlqPayload 는 이벤트를 재구성 가능한 형태로 편 것이다.
//
// 라이브러리 메시지 원문이 아니라 도메인 값을 직렬화한다 —
// 이벤트 포맷이 바뀌거나 라이브러리를 갈아 끼워도 재처리가 깨지지 않는다.
type dlqPayload struct {
	Table      string             `json:"table"`
	Op         string             `json:"op"`
	LSN        uint64             `json:"lsn"`
	SourceTsMs int64              `json:"sourceTsMs"`
	Before     map[string]*string `json:"before"`
	After      map[string]*string `json:"after"`
}

func (r *DeadLetterRepository) toPayload(event model.ChangeEvent) string {
	p := dlqPayload{
		Table:      event.Table,
		Op:         event.Op.Code(),
		LSN:        event.LSN,
		SourceTsMs: event.SourceTsMs,
	}
	if event.Before != nil {
		p.Before = event.Before.Values()
	}
	if event.After != nil {
		p.After = event.After.Values()
	}

	encoded, err := json.Marshal(p)
	if err != nil {
		r.log.Error("DLQ 페이로드 직렬화 실패", "lsn", event.LSN, "error", err)
		fallback, _ := json.Marshal(map[string]any{
			"table": event.Table, "lsn": event.LSN, "serializationFailed": true,
		})
		return string(fallback)
	}
	return string(encoded)
}

// restore 는 toPayload 의 역방향이다. 복원이 되어야 재처리가 성립한다.
func (r *DeadLetterRepository) restore(payload string) (model.ChangeEvent, error) {
	var p dlqPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return model.ChangeEvent{}, err
	}
	op, known := model.OperationFromCode(p.Op)
	if !known {
		return model.ChangeEvent{}, fmt.Errorf("알 수 없는 op: %q", p.Op)
	}

	event := model.ChangeEvent{
		Table:      p.Table,
		Op:         op,
		LSN:        p.LSN,
		SourceTsMs: p.SourceTsMs,
	}
	if p.Before != nil {
		before := model.NewRowData(p.Before)
		event.Before = &before
	}
	if p.After != nil {
		after := model.NewRowData(p.After)
		event.After = &after
	}
	return event, nil
}

// ── 도우미 ──────────────────────────────────────────────────────────────────

// sqlStateOf 는 PostgreSQL 오류코드를 꺼낸다. 없으면 빈 문자열이다.
func sqlStateOf(cause error) *string {
	var pgErr *pgconn.PgError
	if errors.As(cause, &pgErr) {
		code := pgErr.Code
		return &code
	}
	return nil
}

// failureType 은 원인 오류의 성격을 한 줄로 남긴다.
//
// Java 판은 예외 클래스 이름을 썼지만 Go 에는 그에 해당하는 것이 없다.
// PostgreSQL 오류면 코드와 이름을, 아니면 %T 로 얻은 타입 이름을 남긴다.
func failureType(cause error) string {
	var pgErr *pgconn.PgError
	if errors.As(cause, &pgErr) {
		return "pgconn.PgError(" + pgErr.Code + ")"
	}
	if errors.Is(cause, model.ErrBadData) {
		return "model.ErrBadData"
	}
	return fmt.Sprintf("%T", cause)
}

func truncate(text string) string {
	runes := []rune(text)
	if len(runes) <= messageLimit {
		return text
	}
	return string(runes[:messageLimit]) + "...(잘림)"
}
