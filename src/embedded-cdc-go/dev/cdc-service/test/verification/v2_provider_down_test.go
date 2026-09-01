package verification

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// V2. Provider Down Case
//
// 통과 기준
//   - 꺼져 있던 구간의 변경분이 유실 0건으로 도착한다
//   - 꺼져 있는 동안 쌓이는 WAL 량을 측정해 허용 다운타임을 산출한다
func TestV2ProviderDown(t *testing.T) {
	const slotName = "verify_v2_slot"
	const changesWhileDown = 1_000

	Report.Section("V2. Provider 다운 후 재기동")
	CreateRecordFixture()
	TruncateRecords()
	DropSlotQuietly(slotName)
	t.Cleanup(func() { DropSlotQuietly(slotName) })

	// ── 1차 기동: 슬롯을 만들고 진행 지점을 남긴다 ─────────────────────────
	first, err := StartHarness(HarnessOptions{
		Slot:        slotName,
		Publication: RecordPublication,
		Tables:      []string{RecordTable},
		AutoAck:     true,
	})
	if err != nil {
		t.Fatalf("1차 하니스 기동 실패: %v", err)
	}

	InsertRecord("V2-BEFORE-1", "100.00", "NEW")
	if _, ok := first.Poll(10 * time.Second); !ok {
		t.Fatal("1차 기동 중 캡처가 되어야 한다")
	}

	// ack 가 슬롯에 반영될 시간을 준다.
	// Debezium 판에서는 "오프셋 파일 flush" 를 기다렸지만, 여기서 기다리는 것은
	// standby status update 가 서버에 닿는 것이다. 저장 위치만 다르고 역할은 같다.
	time.Sleep(2 * time.Second)
	retainedBeforeDown := SlotRetainedWalBytes(slotName)

	// ── 정지 ──────────────────────────────────────────────────────────────
	first.Stop()
	if SlotActive(slotName) {
		t.Fatal("정지하면 슬롯 점유가 풀려야 한다")
	}
	Report.Note("V2", "Provider 정지 — 슬롯은 남아 있고 active=false 가 된다")

	// ── 꺼져 있는 동안 변경 발생 ───────────────────────────────────────────
	downStart := time.Now()
	InsertRecordsInOneTransaction("V2-DOWN", changesWhileDown)
	// WAL 이 실제로 쌓이도록 잠깐 둔다
	time.Sleep(2 * time.Second)
	retainedWhileDown := SlotRetainedWalBytes(slotName)
	downElapsed := time.Since(downStart)

	walGrowth := retainedWhileDown - max(retainedBeforeDown, 0)
	Report.Metric("V2", "다운 중 변경 건수", changesWhileDown)
	Report.Metric("V2", "다운 시간", downElapsed.Milliseconds())
	Report.Metric("V2", "슬롯이 붙잡은 WAL", HumanBytes(retainedWhileDown))
	Report.Metric("V2", "변경 1건당 WAL 증가", walGrowth/changesWhileDown)

	var keepSize string
	ScalarOnSource(&keepSize, "SHOW max_slot_wal_keep_size")
	Report.Metric("V2", "max_slot_wal_keep_size", keepSize)
	if keepSize == "-1" {
		Report.Note("V2", "max_slot_wal_keep_size=-1 — 상한이 없다. "+
			"Provider 가 오래 죽으면 WAL 이 디스크를 채울 때까지 쌓인다")
	}

	// ── 재기동 ────────────────────────────────────────────────────────────
	second, err := StartHarness(HarnessOptions{
		Slot:        slotName,
		Publication: RecordPublication,
		Tables:      []string{RecordTable},
		AutoAck:     true,
	})
	if err != nil {
		t.Fatalf("재기동 실패: %v", err)
	}
	replayed := second.Collect(changesWhileDown, 3*time.Minute)
	second.Stop()

	// 다운 구간에 넣은 것만 센다. 1차 기동에서 이미 받은 V2-BEFORE-1 이 섞이면
	// "몇 건이 되돌아왔는가" 라는 이 항목의 질문이 흐려진다.
	received := make(map[string]bool, len(replayed))
	for _, c := range replayed {
		if key, ok := Text(c.Event.After, "biz_key"); ok && strings.HasPrefix(key, "V2-DOWN-") {
			received[key] = true
		}
	}

	var missing []string
	for i := 1; i <= changesWhileDown; i++ {
		key := fmt.Sprintf("V2-DOWN-%d", i)
		if !received[key] {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)

	Report.Metric("V2", "재기동 후 수신", fmt.Sprintf("%d / %d", len(received), changesWhileDown))
	Report.Metric("V2", "유실 건수", len(missing))

	if len(missing) > 0 {
		t.Fatalf("꺼져 있던 구간의 변경분은 유실 0건이어야 한다 — %d건 누락 (예: %v)",
			len(missing), missing[:min(5, len(missing))])
	}
	Report.Note("V2", "재기동 시 슬롯의 confirmed_flush_lsn 부터 이어 읽어 유실 없음 — "+
		"슬롯이 WAL 을 붙잡아 준 결과")
}
