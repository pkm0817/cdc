package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
	"github.com/jackc/pgx/v5"
)

// CaptureGap 은 되받을 수 없는 구간이다.
//
//	LastAppliedLSN 우리가 마지막으로 처리한 지점
//	SlotRestartLSN 슬롯이 보관 중인 가장 오래된 지점. 슬롯이 없으면 nil
type CaptureGap struct {
	LastAppliedLSN uint64
	SlotRestartLSN *uint64
	Reason         string
}

func (g CaptureGap) Describe() string {
	return g.Reason + " — 이 구간은 WAL 로 복구할 수 없다. 관심 테이블 재적재(재동기화)가 필요하다."
}

// SlotContinuityGuard 는 캡처 연결고리가 끊겼는지 기동 시점에 판정한다.
//
// 왜 필요한가. Debezium 은 "오프셋 파일은 있는데 슬롯이 없다"를 스스로 잡아내고
// 기동을 거부한다. go-pq-cdc 에는 오프셋 파일이 없다 — 진행 지점은 슬롯의
// confirmed_flush_lsn 하나뿐이라, 슬롯이 사라지면 그 사실을 알 방법이 라이브러리 안에 없다.
// slot.createIfNotExists 가 켜져 있으면 새 슬롯을 만들고 조용히 이어서 돈다.
// 그 사이 변경분은 흔적 없이 사라진다.
//
// 그래서 진행 지점을 수신 측 DB 에도 남기고(CheckpointStore), 기동할 때 슬롯과 대조한다.
// Java 판에서는 보조 수단이었던 이 가드가 여기서는 유일한 탐지 수단이다.
//
// 판정 규칙 — 둘 중 하나라도 걸리면 되받을 수 없는 구간이 생긴 것이다.
//  1. 처리 이력은 있는데 슬롯이 없다
//  2. 슬롯의 restart_lsn 이 마지막으로 처리한 지점보다 앞서 있다
type SlotContinuityGuard struct {
	sourceDSN   string
	slotName    string
	pipeline    string
	checkpoints port.CheckpointStore
	log         *slog.Logger
}

func NewSlotContinuityGuard(
	sourceDSN, slotName, pipeline string,
	checkpoints port.CheckpointStore,
	log *slog.Logger,
) *SlotContinuityGuard {
	return &SlotContinuityGuard{
		sourceDSN:   sourceDSN,
		slotName:    slotName,
		pipeline:    pipeline,
		checkpoints: checkpoints,
		log:         log,
	}
}

// DetectGap 은 되받을 수 없는 구간이 있으면 그 내용을, 없으면 nil 을 돌려준다.
func (g *SlotContinuityGuard) DetectGap(ctx context.Context) (*CaptureGap, error) {
	lastApplied, hasHistory, err := g.checkpoints.LastAppliedLSN(ctx, g.pipeline)
	if err != nil {
		return nil, fmt.Errorf("진행 지점을 읽지 못했다: %w", err)
	}
	if !hasHistory {
		g.log.Info("진행 기록이 없다 — 최초 기동으로 본다", "pipeline", g.pipeline)
		return nil, nil
	}

	exists, restartLSN, err := g.readSlotState(ctx)
	if err != nil {
		return nil, err
	}

	if !exists {
		return &CaptureGap{
			LastAppliedLSN: lastApplied,
			Reason: fmt.Sprintf("처리 이력(LSN %s)은 있는데 복제 슬롯 %q 이 없다. "+
				"슬롯이 삭제됐거나 DB 가 교체됐다", FormatLSN(lastApplied), g.slotName),
		}, nil
	}

	if restartLSN > lastApplied {
		restart := restartLSN
		return &CaptureGap{
			LastAppliedLSN: lastApplied,
			SlotRestartLSN: &restart,
			Reason: fmt.Sprintf("슬롯이 이미 %s 까지 버렸는데 우리는 %s 까지만 처리했다. "+
				"그 사이는 되받을 수 없다", FormatLSN(restartLSN), FormatLSN(lastApplied)),
		}, nil
	}

	g.log.Info("캡처 연결고리 정상",
		"lastApplied", FormatLSN(lastApplied), "slotRestartLsn", FormatLSN(restartLSN))
	return nil, nil
}

func (g *SlotContinuityGuard) readSlotState(ctx context.Context) (exists bool, restartLSN uint64, err error) {
	conn, err := pgx.Connect(ctx, g.sourceDSN)
	if err != nil {
		return false, 0, fmt.Errorf(
			"복제 슬롯 상태를 읽지 못했다 — 연결고리를 확인할 수 없으므로 기동하지 않는다: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	var restartText *string
	scanErr := conn.QueryRow(ctx,
		`SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1`,
		g.slotName).Scan(&restartText)
	if errors.Is(scanErr, pgx.ErrNoRows) {
		return false, 0, nil
	}
	if scanErr != nil {
		return false, 0, fmt.Errorf("복제 슬롯 상태 조회 실패: %w", scanErr)
	}

	// 슬롯은 있는데 restart_lsn 이 null 인 경우가 있다(아직 한 번도 사용되지 않은 슬롯).
	// 버린 구간이 없다는 뜻이므로 0 으로 본다.
	if restartText == nil {
		return true, 0, nil
	}
	value, parseErr := ParseLSN(*restartText)
	if parseErr != nil {
		return false, 0, parseErr
	}
	return true, value, nil
}

// ParseLSN 은 PostgreSQL LSN 표기("0/1A2B3C4")를 비교 가능한 정수로 바꾼다.
func ParseLSN(lsn string) (uint64, error) {
	slash := strings.Index(lsn, "/")
	if slash < 0 {
		return 0, fmt.Errorf("LSN 형식이 아니다: %q", lsn)
	}
	high, err := strconv.ParseUint(lsn[:slash], 16, 64)
	if err != nil {
		return 0, fmt.Errorf("LSN 상위를 읽지 못했다: %q: %w", lsn, err)
	}
	low, err := strconv.ParseUint(lsn[slash+1:], 16, 64)
	if err != nil {
		return 0, fmt.Errorf("LSN 하위를 읽지 못했다: %q: %w", lsn, err)
	}
	return high<<32 | low, nil
}

// FormatLSN 은 정수 LSN 을 사람이 읽는 표기로 되돌린다.
// 로그와 경보에서 pg_replication_slots 조회 결과와 바로 대조된다.
func FormatLSN(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}
