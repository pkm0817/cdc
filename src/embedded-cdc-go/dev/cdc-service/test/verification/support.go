package verification

import (
	"fmt"
	"sort"
	"time"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/infrastructure/cdc"
)

const (
	// RecordTable 은 실적 레코드를 본뜬 검증 테이블이다.
	RecordTable = "verify_record"
	// RecordPublication 은 그 표만 담은 검증 전용 publication 이다.
	RecordPublication = "verify_record_pub"
)

// CreateRecordFixture 는 검증 테이블과 publication 을 만든다.
//
// payload 는 TOAST 임계(약 2KB)를 넘길 수 있는 대용량 필드로, V5 에서 쓴다.
func CreateRecordFixture() {
	OnSource(`
CREATE TABLE IF NOT EXISTS verify_record (
    id          BIGSERIAL     PRIMARY KEY,
    biz_key     TEXT          NOT NULL,
    amount      NUMERIC(14,2) NOT NULL,
    status      TEXT          NOT NULL,
    payload     TEXT,
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
)`)
	// UPDATE/DELETE 이벤트의 old 이미지에 전체 컬럼을 담기 위한 설정.
	// 기본값(DEFAULT)이면 old 에 PK 만 실려 필드 단위 변경 식별이 불가능하다.
	OnSource("ALTER TABLE verify_record REPLICA IDENTITY FULL")
	RecreatePublication(RecordPublication, RecordTable)
}

// RecreatePublication 은 publication 을 지우고 다시 만든다.
func RecreatePublication(publicationName string, tables ...string) {
	OnSource("DROP PUBLICATION IF EXISTS " + publicationName)
	list := ""
	for i, t := range tables {
		if i > 0 {
			list += ", "
		}
		list += t
	}
	OnSource("CREATE PUBLICATION " + publicationName + " FOR TABLE " + list)
}

// TruncateRecords 는 검증 데이터를 지운다.
//
// TRUNCATE 는 이 파이프라인의 관심 밖(op=t)이라 캡처되지 않는다.
// 정리 작업이 이벤트로 흘러 들어와 다음 시나리오를 오염시키지 않으므로 오히려 그 편이 낫다.
func TruncateRecords() {
	OnSource("TRUNCATE TABLE verify_record RESTART IDENTITY")
}

// ── 복제 슬롯 관측 ──────────────────────────────────────────────────────────

func SlotExists(slotName string) bool {
	return CountOnSource(
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slotName) > 0
}

// DropSlotQuietly 는 슬롯을 지운다. 없으면 아무 일도 하지 않는다.
func DropSlotQuietly(slotName string) {
	if !SlotExists(slotName) {
		return
	}
	// active 인 슬롯은 지워지지 않는다. 아직 붙어 있다는 뜻이므로 잠깐 뒤 다시 시도한다.
	for attempt := 0; attempt < 10; attempt++ {
		_, err := Source().Exec(ctxBackground(), "SELECT pg_drop_replication_slot($1)", slotName)
		if err == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
		if !SlotExists(slotName) {
			return
		}
	}
	panic(fmt.Sprintf("슬롯 %q 을 지우지 못했다 — 아직 점유 중일 수 있다", slotName))
}

// SlotRetainedWalBytes 는 슬롯이 붙잡고 있어 아직 지우지 못하는 WAL 크기다.
// 허용 다운타임 산출의 근거가 된다.
func SlotRetainedWalBytes(slotName string) int64 {
	var bytes *int64
	found := ScalarOnSource(&bytes, `
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
  FROM pg_replication_slots WHERE slot_name = $1`, slotName)
	if !found || bytes == nil {
		return -1
	}
	return *bytes
}

// SlotConfirmedFlushLSN 은 슬롯이 확정한 지점이다. 빈 문자열이면 아직 없다.
func SlotConfirmedFlushLSN(slotName string) string {
	var lsn *string
	found := ScalarOnSource(&lsn, `
SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, slotName)
	if !found || lsn == nil {
		return ""
	}
	return *lsn
}

func SlotRestartLSN(slotName string) string {
	var lsn *string
	found := ScalarOnSource(&lsn, `
SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, slotName)
	if !found || lsn == nil {
		return ""
	}
	return *lsn
}

func SlotActive(slotName string) bool {
	var active *bool
	found := ScalarOnSource(&active, `
SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slotName)
	return found && active != nil && *active
}

// LSNDiff 는 두 LSN 표기의 차(바이트)를 돌려준다. 슬롯이 얼마나 전진했는지 재는 데 쓴다.
func LSNDiff(after, before string) int64 {
	if after == "" || before == "" {
		return -1
	}
	a, err := cdc.ParseLSN(after)
	if err != nil {
		return -1
	}
	b, err := cdc.ParseLSN(before)
	if err != nil {
		return -1
	}
	return int64(a) - int64(b)
}

// ── 데이터 투입 ─────────────────────────────────────────────────────────────

func InsertRecord(bizKey, amount, status string) {
	if _, err := Source().Exec(ctxBackground(),
		"INSERT INTO verify_record (biz_key, amount, status) VALUES ($1, $2::numeric, $3)",
		bizKey, amount, status); err != nil {
		panic(fmt.Sprintf("검증 행 삽입 실패: %v", err))
	}
}

// InsertRecordsInOneTransaction 은 한 트랜잭션으로 n 건을 넣는다. 대량 변경 처리량 측정용이다.
func InsertRecordsInOneTransaction(prefix string, n int) {
	ctx := ctxBackground()
	tx, err := Source().Begin(ctx)
	if err != nil {
		panic(fmt.Sprintf("트랜잭션 시작 실패: %v", err))
	}
	for i := 1; i <= n; i++ {
		if _, err := tx.Exec(ctx,
			"INSERT INTO verify_record (biz_key, amount, status) VALUES ($1, $2::numeric, $3)",
			fmt.Sprintf("%s-%d", prefix, i), fmt.Sprintf("%d", 1000+i), "NEW"); err != nil {
			_ = tx.Rollback(ctx)
			panic(fmt.Sprintf("대량 INSERT 실패: %v", err))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		panic(fmt.Sprintf("대량 INSERT 커밋 실패: %v", err))
	}
}

// ── 이벤트 판독 ─────────────────────────────────────────────────────────────

// ChangedFields 는 before 와 after 를 비교해 "실제로 바뀐 필드"만 뽑는다.
// V1 의 필드 단위 변경 식별이 가능한지가 이 함수가 값을 돌려주느냐로 판정된다.
func ChangedFields(event model.ChangeEvent) []string {
	if event.Before == nil || event.After == nil {
		return nil
	}
	before := event.Before.Values()
	after := event.After.Values()

	seen := make(map[string]bool)
	for k := range before {
		seen[k] = true
	}
	for k := range after {
		seen[k] = true
	}

	var changed []string
	for column := range seen {
		b, bok := before[column]
		a, aok := after[column]
		if bok != aok || !sameText(b, a) {
			changed = append(changed, column)
		}
	}
	sort.Strings(changed)
	return changed
}

func sameText(a, b *string) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

// Text 는 이벤트 행에서 문자열 컬럼을 꺼낸다. 없으면 빈 문자열과 false.
func Text(row *model.RowData, column string) (string, bool) {
	if row == nil {
		return "", false
	}
	value, present := row.Values()[column]
	if !present || value == nil {
		return "", false
	}
	return *value, true
}

// Contains 는 문자열 목록에 값이 있는지 본다.
func Contains(list []string, value string) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

// HumanBytes 는 바이트 수를 읽기 좋은 표기로 바꾼다.
func HumanBytes(bytes int64) string {
	switch {
	case bytes < 0:
		return "측정 불가"
	case bytes < 1024:
		return fmt.Sprintf("%d B", bytes)
	case bytes < 1024*1024:
		return fmt.Sprintf("%.1f KiB", float64(bytes)/1024)
	default:
		return fmt.Sprintf("%.2f MiB", float64(bytes)/(1024*1024))
	}
}
