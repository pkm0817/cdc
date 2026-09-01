package verification

import (
	"testing"
	"time"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
)

// V1. 기본 캡처
//
// 통과 기준
//   - INSERT/UPDATE/DELETE 가 목표 지연 안에 도착한다 (기본 5초)
//   - UPDATE 에서 어떤 필드가 무엇에서 무엇으로 바뀌었는지 식별된다
//   - 1만 건 배치에서도 밀리지 않는다 (처리량 측정)
func TestV1BasicCapture(t *testing.T) {
	const slotName = "verify_v1_slot"
	// 목표 지연. 이 값을 넘으면 실패로 본다.
	const latencyBudget = 5 * time.Second

	Report.Section("V1. 기본 캡처")
	CreateRecordFixture()
	TruncateRecords()
	DropSlotQuietly(slotName)

	harness, err := StartHarness(HarnessOptions{
		Slot:        slotName,
		Publication: RecordPublication,
		Tables:      []string{RecordTable},
		AutoAck:     true,
	})
	if err != nil {
		t.Fatalf("하니스 기동 실패: %v", err)
	}
	t.Cleanup(func() {
		harness.Stop()
		DropSlotQuietly(slotName)
	})

	t.Run("INSERT 가 목표 지연 안에 도착한다", func(t *testing.T) {
		harness.Drain()
		sentAt := time.Now()
		InsertRecord("V1-INS-1", "15000.00", "NEW")

		captured, ok := harness.Poll(latencyBudget)
		if !ok {
			t.Fatalf("INSERT 이벤트가 %s 안에 도착해야 한다", latencyBudget)
		}
		event := captured.Event
		if event.Op != model.Create {
			t.Fatalf("op 가 c 여야 한다: %s", event.Op.Code())
		}
		if key, _ := Text(event.After, "biz_key"); key != "V1-INS-1" {
			t.Fatalf("biz_key 가 다르다: %q", key)
		}
		if amount, _ := Text(event.After, "amount"); amount != "15000.00" {
			t.Fatalf("amount 가 다르다: %q", amount)
		}

		Report.Metric("V1", "INSERT 왕복 지연(측정 기준)", time.Since(sentAt).Milliseconds())

		// MessageTime 은 DB 서버 시계, time.Now() 는 이 프로세스의 시계다.
		// 둘이 어긋나 있으면 "송신 기준 지연"이 음수로도 나온다 — 지연이 아니라 시계 편차다.
		// 운영의 cdc_end_to_end_lag_seconds 가 같은 방식으로 계산되므로 편차를 반드시 남긴다.
		var dbNowMs int64
		ScalarOnSource(&dbNowMs, "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint")
		skew := dbNowMs - time.Now().UnixMilli()
		Report.Metric("V1", "DB 서버와 측정 프로세스의 시계 편차", skew)
		Report.Metric("V1", "INSERT 송신 기준 지연(편차 보정)",
			time.Now().UnixMilli()+skew-event.SourceTsMs)

		if skew > 1000 || skew < -1000 {
			Report.Note("V1", "주의: 시계 편차가 크다. 송신 시각 기반 지연 지표는 "+
				"DB 와 Provider 의 시계가 동기화된 환경에서만 신뢰할 수 있다")
		}

		// Debezium 의 source.ts_ms 는 "커밋 시각"이지만 go-pq-cdc 가 주는
		// MessageTime 은 "WAL 송신 시각"이다. 커밋 직후에 보내므로 실무상 차이는 작지만,
		// 두 스택의 지연 지표를 같은 값으로 취급하면 안 된다.
		Report.Note("V1", "지연의 기준 시각이 Debezium 판과 다르다 — "+
			"Debezium 은 커밋 시각(source.ts_ms), go-pq-cdc 는 WAL 송신 시각(MessageTime)")
	})

	t.Run("UPDATE 에서 바뀐 필드를 값까지 식별할 수 있다", func(t *testing.T) {
		harness.Drain()
		InsertRecord("V1-UPD-1", "20000.00", "NEW")
		if _, ok := harness.Poll(latencyBudget); !ok {
			t.Fatal("선행 INSERT 이벤트가 도착해야 한다")
		}

		sentAt := time.Now()
		OnSource("UPDATE verify_record SET status = 'CONFIRMED', amount = 22000.00 " +
			"WHERE biz_key = 'V1-UPD-1'")

		captured, ok := harness.Poll(latencyBudget)
		if !ok {
			t.Fatal("UPDATE 이벤트가 도착해야 한다")
		}
		event := captured.Event
		if event.Op != model.Update {
			t.Fatalf("op 가 u 여야 한다: %s", event.Op.Code())
		}

		// 핵심: before 가 비어 있으면 "무엇에서 무엇으로" 를 말할 수 없다
		if event.Before == nil {
			t.Fatal("REPLICA IDENTITY FULL 이면 before 가 채워진다")
		}
		if v, _ := Text(event.Before, "status"); v != "NEW" {
			t.Fatalf("before.status 가 다르다: %q", v)
		}
		if v, _ := Text(event.After, "status"); v != "CONFIRMED" {
			t.Fatalf("after.status 가 다르다: %q", v)
		}
		if v, _ := Text(event.Before, "amount"); v != "20000.00" {
			t.Fatalf("before.amount 가 다르다: %q", v)
		}
		if v, _ := Text(event.After, "amount"); v != "22000.00" {
			t.Fatalf("after.amount 가 다르다: %q", v)
		}

		changed := ChangedFields(event)
		if !Contains(changed, "status") || !Contains(changed, "amount") {
			t.Fatalf("바뀐 필드가 정확히 뽑히지 않았다: %v", changed)
		}
		if Contains(changed, "biz_key") || Contains(changed, "id") {
			t.Fatalf("건드리지 않은 필드가 변경으로 잡혔다: %v", changed)
		}

		Report.Metric("V1", "UPDATE 지연", time.Since(sentAt).Milliseconds())
		Report.Metric("V1", "식별된 변경 필드", changed)
		Report.Note("V1", "status: NEW -> CONFIRMED, amount: 20000.00 -> 22000.00 판독 성공")
	})

	t.Run("DELETE 의 before 로 삭제된 행 전체를 알 수 있다", func(t *testing.T) {
		harness.Drain()
		InsertRecord("V1-DEL-1", "30000.00", "NEW")
		if _, ok := harness.Poll(latencyBudget); !ok {
			t.Fatal("선행 INSERT 이벤트가 도착해야 한다")
		}

		sentAt := time.Now()
		OnSource("DELETE FROM verify_record WHERE biz_key = 'V1-DEL-1'")

		captured, ok := harness.Poll(latencyBudget)
		if !ok {
			t.Fatal("DELETE 이벤트가 도착해야 한다")
		}
		event := captured.Event
		if event.Op != model.Delete {
			t.Fatalf("op 가 d 여야 한다: %s", event.Op.Code())
		}
		if event.After != nil {
			t.Fatal("DELETE 에는 after 가 없다")
		}
		if v, _ := Text(event.Before, "biz_key"); v != "V1-DEL-1" {
			t.Fatalf("before.biz_key 가 다르다: %q", v)
		}
		if v, _ := Text(event.Before, "amount"); v != "30000.00" {
			t.Fatalf("before.amount 가 다르다: %q", v)
		}

		Report.Metric("V1", "DELETE 지연", time.Since(sentAt).Milliseconds())
	})

	t.Run("1만 건 배치에서도 유실 없이 따라온다", func(t *testing.T) {
		const batchSize = 10_000
		harness.Drain()

		start := time.Now()
		InsertRecordsInOneTransaction("V1-BULK", batchSize)
		committedAt := time.Now()

		captured := harness.Collect(batchSize, 3*time.Minute)
		drain := time.Since(committedAt)

		throughput := int64(batchSize)
		if drain.Milliseconds() > 0 {
			throughput = int64(batchSize) * 1000 / drain.Milliseconds()
		}

		Report.Metric("V1", "배치 건수", batchSize)
		Report.Metric("V1", "source 커밋 소요", committedAt.Sub(start).Milliseconds())
		Report.Metric("V1", "캡처 완료까지", drain.Milliseconds())
		Report.Metric("V1", "캡처 처리량", throughput)
		Report.Metric("V1", "수신 건수", len(captured))

		if len(captured) != batchSize {
			t.Fatalf("1만 건이 하나도 빠지지 않고 도착해야 한다: %d / %d", len(captured), batchSize)
		}

		// 순서 보존도 함께 확인한다 — LSN 은 단조 증가해야 한다
		var previous uint64
		for i, c := range captured {
			if c.Event.Op != model.Create {
				t.Fatalf("%d번째 이벤트의 op 가 c 가 아니다: %s", i, c.Event.Op.Code())
			}
			if i > 0 && c.Event.LSN <= previous {
				t.Fatalf("LSN 이 역전됐다 (%d번째): %d <= %d", i, c.Event.LSN, previous)
			}
			previous = c.Event.LSN
		}
		Report.Note("V1", "1만 건 LSN 단조 증가 확인 — WAL 순서 보존됨")
	})
}
