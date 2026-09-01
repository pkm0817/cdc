package verification

import (
	"testing"
	"time"
)

// V4. 같은 이벤트가 두 번 오는 경우 — 이중 반영 제한
//
// 논리 복제는 at-least-once 다. 확정(ack)하기 전에 죽으면 그 구간이 다시 온다.
// 이것은 결함이 아니라 정상 동작이므로, 막을 대상은 "중복 수신"이 아니라 "중복 반영"이다.
//
// ── 중복을 만드는 방법이 Debezium 판과 다르다 ────────────────────────────────
// Debezium 판은 오프셋 파일을 체크포인트로 되돌려 "flush 전에 죽은 상태"를 재현했다.
// 여기에는 오프셋 파일이 없다. 대신 하니스를 수동 ack 모드로 띄우고
// 일부러 ack 를 보내지 않는다 — 그것이 곧 "확정하지 못한 채 죽은 상태"다.
// 재현되는 코드 경로는 같다: 슬롯의 confirmed_flush_lsn 이 뒤처진 채 재접속한다.
//
// 통과 기준
//   - 중복이 실제로 유입된다(= at-least-once 임을 확인)
//   - 그럼에도 최종 데이터 정합이 유지된다
func TestV4DuplicateDelivery(t *testing.T) {
	const slotName = "verify_v4_slot"
	const records = 200

	Report.Section("V4. 중복 유입과 이중 반영 방지")
	CreateRecordFixture()
	TruncateRecords()
	DropSlotQuietly(slotName)
	t.Cleanup(func() { DropSlotQuietly(slotName) })

	OnTarget(`
CREATE TABLE IF NOT EXISTS verify_sink (
    id          BIGINT        PRIMARY KEY,
    biz_key     TEXT          NOT NULL,
    amount      NUMERIC(14,2) NOT NULL,
    apply_count INT           NOT NULL DEFAULT 1,
    source_lsn  BIGINT        NOT NULL
)`)
	OnTarget("TRUNCATE TABLE verify_sink")

	// ── 1차 기동: 수동 ack ────────────────────────────────────────────────
	first, err := StartHarness(HarnessOptions{
		Slot:        slotName,
		Publication: RecordPublication,
		Tables:      []string{RecordTable},
		AutoAck:     false, // 테스트가 ack 시점을 직접 정한다
	})
	if err != nil {
		t.Fatalf("1차 하니스 기동 실패: %v", err)
	}

	// ── 선행 구간: 여기까지는 확정(ack)한다 ───────────────────────────────
	InsertRecordsInOneTransaction("V4-BASE", 20)
	base := first.Collect(20, time.Minute)
	if len(base) != 20 {
		t.Fatalf("선행 구간 20건이 도착해야 한다: %d", len(base))
	}
	AckAll(base)
	time.Sleep(2 * time.Second) // ack 가 슬롯에 닿을 시간
	Report.Note("V4", "선행 구간까지 ack — 이 지점이 슬롯에 확정된 마지막 위치가 된다")

	// ── 이 구간의 이벤트가 중복 대상이 된다 ────────────────────────────────
	first.Drain()
	InsertRecordsInOneTransaction("V4", records)
	firstRound := first.Collect(records, 2*time.Minute)
	if len(firstRound) != records {
		t.Fatalf("1차 수신이 %d건이어야 한다: %d", records, len(firstRound))
	}

	appliedFirst := applyAll(firstRound)

	// ack 를 보내지 않고 내린다 — 확정 전에 죽은 것과 같은 상태다.
	first.Stop()
	Report.Note("V4", "1차 구간을 ack 하지 않고 정지 — 확정 전에 죽은 상황과 동일한 상태")

	// ── 재기동: 확정되지 않은 지점부터 다시 온다 ───────────────────────────
	second, err := StartHarness(HarnessOptions{
		Slot:        slotName,
		Publication: RecordPublication,
		Tables:      []string{RecordTable},
		AutoAck:     true,
	})
	if err != nil {
		t.Fatalf("재기동 실패: %v", err)
	}
	secondRound := second.Collect(records, 2*time.Minute)
	second.Stop()

	firstLSNs := make(map[uint64]bool, len(firstRound))
	for _, c := range firstRound {
		firstLSNs[c.Event.LSN] = true
	}
	duplicates := 0
	for _, c := range secondRound {
		if firstLSNs[c.Event.LSN] {
			duplicates++
		}
	}

	Report.Metric("V4", "1차 수신", len(firstRound))
	Report.Metric("V4", "재기동 후 수신", len(secondRound))
	Report.Metric("V4", "그중 중복(같은 LSN)", duplicates)

	appliedSecond := applyAll(secondRound)
	Report.Metric("V4", "1차 반영 행 수", appliedFirst)
	Report.Metric("V4", "2차 반영 행 수", appliedSecond)

	// ── 최종 정합 판정 ────────────────────────────────────────────────────
	sinkRows := CountOnTarget("SELECT count(*) FROM verify_sink")
	doubleApplied := CountOnTarget("SELECT count(*) FROM verify_sink WHERE apply_count > 1")
	// 선행 구간(V4-BASE-*)은 sink 에 반영하지 않았으므로 비교에서 뺀다
	sourceRows := CountOnSource(
		"SELECT count(*) FROM verify_record WHERE biz_key LIKE 'V4-%' AND biz_key NOT LIKE 'V4-BASE-%'")

	Report.Metric("V4", "원본 건수", sourceRows)
	Report.Metric("V4", "수신측 건수", sinkRows)
	Report.Metric("V4", "두 번 반영된 행", doubleApplied)

	if duplicates == 0 {
		t.Fatal("at-least-once 이므로 중복이 실제로 와야 한다")
	}
	if sinkRows != sourceRows {
		t.Fatalf("중복이 와도 행이 늘지 않아야 한다: 수신 %d / 원본 %d", sinkRows, sourceRows)
	}
	if doubleApplied != 0 {
		t.Fatalf("같은 실적이 두 번 반영되면 안 된다: %d건", doubleApplied)
	}

	Report.Note("V4", "LSN 가드가 붙은 멱등 UPSERT 라 중복 이벤트는 갱신 행 수 0 으로 차단된다")
}

// applyAll 은 운영의 computer/grade/member 저장소와 같은 규칙으로 반영한다 —
// 멱등 UPSERT + source_lsn 가드. 더 오래되거나 같은 LSN 은 반영되지 않는다.
//
// apply_count 를 세는 것이 이 함수의 핵심이다. 가드가 없으면 중복이 올 때마다
// 이 값이 올라가고, 그것이 곧 "같은 실적이 두 번 반영됐다"는 증거가 된다.
func applyAll(captured []Captured) int64 {
	var applied int64
	for _, c := range captured {
		if c.Event.After == nil {
			continue
		}
		id, err := c.Event.After.Int64("id")
		if err != nil {
			continue
		}
		bizKey, _ := Text(c.Event.After, "biz_key")
		amount, _ := Text(c.Event.After, "amount")

		applied += UpdateOnTarget(`
INSERT INTO verify_sink (id, biz_key, amount, apply_count, source_lsn)
VALUES ($1, $2, $3::numeric, 1, $4)
ON CONFLICT (id) DO UPDATE SET
    biz_key     = EXCLUDED.biz_key,
    amount      = EXCLUDED.amount,
    apply_count = verify_sink.apply_count + 1,
    source_lsn  = EXCLUDED.source_lsn
WHERE EXCLUDED.source_lsn > verify_sink.source_lsn`,
			id, bizKey, amount, int64(c.Event.LSN))
	}
	return applied
}
