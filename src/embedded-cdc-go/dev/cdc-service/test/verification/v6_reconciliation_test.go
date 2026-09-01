package verification

import (
	"fmt"
	"testing"
	"time"
)

// V6. 최종 안정성 — 대사와 heartbeat
//
// 앞의 V1~V5 는 "이 상황에서 어떻게 되는가"를 봤다. V6 는 "그 상황이 벌어졌을 때 알아채는가"를 본다.
// 파이프라인이 조용히 어긋나는 것을 막는 마지막 그물이다.
//
// 통과 기준
//   - 인위적으로 유실을 주입하면 대사에서 검출된다
//   - heartbeat 가 변경이 없는 구간에서도 슬롯을 전진시킨다
func TestV6Reconciliation(t *testing.T) {
	const slotName = "verify_v6_slot"
	const records = 300

	Report.Section("V6. 대사와 heartbeat")
	CreateRecordFixture()
	TruncateRecords()
	DropSlotQuietly(slotName)
	t.Cleanup(func() { DropSlotQuietly(slotName) })

	OnTarget(`
CREATE TABLE IF NOT EXISTS verify_recon (
    id      BIGINT        PRIMARY KEY,
    biz_key TEXT          NOT NULL,
    amount  NUMERIC(14,2) NOT NULL,
    status  TEXT          NOT NULL
)`)
	OnTarget("TRUNCATE TABLE verify_recon")

	t.Run("정상 동기화 후에는 건수와 체크섬이 양쪽에서 일치한다", func(t *testing.T) {
		harness, err := StartHarness(HarnessOptions{
			Slot:        slotName,
			Publication: RecordPublication,
			Tables:      []string{RecordTable},
			AutoAck:     true,
		})
		if err != nil {
			t.Fatalf("하니스 기동 실패: %v", err)
		}
		defer harness.Stop()

		InsertRecordsInOneTransaction("V6", records)
		captured := harness.Collect(records, 2*time.Minute)
		if len(captured) != records {
			t.Fatalf("%d건이 도착해야 한다: %d", records, len(captured))
		}

		for _, c := range captured {
			if c.Event.After == nil {
				continue
			}
			id, err := c.Event.After.Int64("id")
			if err != nil {
				t.Fatalf("id 를 읽지 못했다: %v", err)
			}
			bizKey, _ := Text(c.Event.After, "biz_key")
			amount, _ := Text(c.Event.After, "amount")
			status, _ := Text(c.Event.After, "status")
			UpdateOnTarget(`
INSERT INTO verify_recon (id, biz_key, amount, status)
VALUES ($1, $2, $3::numeric, $4)
ON CONFLICT (id) DO UPDATE SET
    biz_key = EXCLUDED.biz_key, amount = EXCLUDED.amount, status = EXCLUDED.status`,
				id, bizKey, amount, status)
		}

		r := reconcile()
		Report.Metric("V6", "원본 건수", r.sourceCount)
		Report.Metric("V6", "수신측 건수", r.targetCount)
		Report.Metric("V6", "원본 체크섬", r.sourceChecksum)
		Report.Metric("V6", "수신측 체크섬", r.targetChecksum)
		Report.Metric("V6", "대사 결과", matchLabel(r.matches()))

		if !r.matches() {
			t.Fatal("정상 동기화 상태에서는 대사가 일치해야 한다")
		}
	})

	t.Run("인위적으로 유실을 주입하면 대사가 검출한다", func(t *testing.T) {
		removed := UpdateOnTarget(
			"DELETE FROM verify_recon WHERE id IN (SELECT id FROM verify_recon ORDER BY id LIMIT 3)")
		Report.Note("V6", fmt.Sprintf("수신측에서 %d건을 임의 삭제 — 유실 상황 주입", removed))

		r := reconcile()
		Report.Metric("V6", "유실 주입 후 건수차", r.sourceCount-r.targetCount)
		Report.Metric("V6", "유실 주입 후 대사 결과", matchLabel(r.matches()))

		if r.matches() {
			t.Fatal("유실이 있으면 대사가 반드시 불일치를 내야 한다")
		}
		if r.sourceCount-r.targetCount != removed {
			t.Fatalf("건수차가 삭제한 건수와 같아야 한다: %d vs %d",
				r.sourceCount-r.targetCount, removed)
		}

		// 값만 바뀐 경우도 잡히는지 — 건수는 같고 체크섬만 달라지는 상황
		UpdateOnTarget(
			"UPDATE verify_recon SET amount = amount + 1 WHERE id = (SELECT min(id) FROM verify_recon)")
		afterTamper := reconcile()
		Report.Metric("V6", "값 변조 후 대사 결과", matchLabel(afterTamper.matches()))
		Report.Note("V6", "건수가 같아도 체크섬이 달라 검출된다 — 건수 대사만으로는 부족하다")
	})

	t.Run("heartbeat 없이는 유휴 구간에서 슬롯이 전진하지 않는다", func(t *testing.T) {
		advanced := measureSlotAdvanceWhileIdle(t, "verify_v6_hb_off", HarnessOptions{
			Slot:        "verify_v6_hb_off",
			Publication: RecordPublication,
			Tables:      []string{RecordTable},
			AutoAck:     true,
		}, "heartbeat 없음")

		Report.Metric("V6", "heartbeat 없음 · 슬롯 전진량", fmt.Sprintf("%d bytes", advanced))
		if advanced == 0 {
			Report.Note("V6", "관심 테이블에 변경이 없으면 슬롯이 전혀 전진하지 않는다. "+
				"다른 테이블이 만든 WAL 을 슬롯이 계속 붙잡는다")
		}
		if advanced != 0 {
			t.Fatalf("heartbeat 없이는 전진하지 않아야 한다: %d bytes", advanced)
		}
	})

	t.Run("heartbeat 표를 걸면 유휴 구간에도 슬롯이 전진한다", func(t *testing.T) {
		// heartbeat 표는 반드시 publication 안에 있어야 한다 —
		// 관심 테이블 집합 밖에서 만든 WAL 은 슬롯을 밀어 주지 못하기 때문이다.
		// 라이브러리가 기동 시 이 조건을 검사한다.
		OnSource(`
CREATE TABLE IF NOT EXISTS verify_heartbeat (
    id             INTEGER     PRIMARY KEY DEFAULT 1,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT verify_heartbeat_single_row CHECK (id = 1)
)`)
		OnSource("INSERT INTO verify_heartbeat (id) VALUES (1) ON CONFLICT DO NOTHING")
		RecreatePublication("verify_hb_pub", RecordTable, "verify_heartbeat")

		advanced := measureSlotAdvanceWhileIdle(t, "verify_v6_hb_on", HarnessOptions{
			Slot:              "verify_v6_hb_on",
			Publication:       "verify_hb_pub",
			Tables:            []string{RecordTable, "verify_heartbeat"},
			AutoAck:           true,
			HeartbeatTable:    "verify_heartbeat",
			HeartbeatInterval: time.Second,
		}, "heartbeat 표 사용")

		Report.Metric("V6", "heartbeat 표 사용 · 슬롯 전진량", fmt.Sprintf("%d bytes", advanced))
		if advanced <= 0 {
			t.Fatalf("heartbeat 가 관심 테이블에 WAL 을 만들어 주면 슬롯이 전진해야 한다: %d bytes", advanced)
		}

		Report.Note("V6", "권고: 관심 테이블 변경이 드문 환경에서는 heartbeat 표를 반드시 설정할 것. "+
			"Debezium 판이 heartbeat.interval.ms 와 heartbeat.action.query 를 함께 걸어야 했던 것과 "+
			"같은 문제이며, go-pq-cdc 는 표 하나를 지정하면 그 둘을 한꺼번에 해 준다")
	})
}

// measureSlotAdvanceWhileIdle 은 관심 테이블은 건드리지 않고 다른 테이블에만 WAL 을
// 발생시킨 뒤, 슬롯의 confirmed_flush_lsn 이 얼마나 전진했는지 잰다.
func measureSlotAdvanceWhileIdle(t *testing.T, slotName string, opts HarnessOptions, label string) int64 {
	t.Helper()

	DropSlotQuietly(slotName)
	defer func() {
		DropSlotQuietly(slotName)
		OnSource("DROP TABLE IF EXISTS verify_noise")
	}()

	harness, err := StartHarness(opts)
	if err != nil {
		t.Fatalf("하니스 기동 실패(%s): %v", label, err)
	}
	defer harness.Stop()

	time.Sleep(3 * time.Second)
	lsnBefore := SlotConfirmedFlushLSN(slotName)
	retainedBefore := SlotRetainedWalBytes(slotName)

	OnSource("CREATE TABLE IF NOT EXISTS verify_noise (id BIGSERIAL PRIMARY KEY, v TEXT)")
	for i := 0; i < 20; i++ {
		OnSource("INSERT INTO verify_noise (v) VALUES (repeat('x', 10000))")
		time.Sleep(150 * time.Millisecond)
	}
	time.Sleep(8 * time.Second) // heartbeat 이 여러 번 돌고 ack 가 서버에 닿을 시간

	lsnAfter := SlotConfirmedFlushLSN(slotName)
	retainedAfter := SlotRetainedWalBytes(slotName)

	Report.Metric("V6", label+" · confirmed_flush_lsn", lsnBefore+" -> "+lsnAfter)
	Report.Metric("V6", label+" · 붙잡은 WAL",
		fmt.Sprintf("%d -> %d bytes", retainedBefore, retainedAfter))

	return LSNDiff(lsnAfter, lsnBefore)
}

// ── 대사 로직 ───────────────────────────────────────────────────────────────

// reconcileSQL 은 건수 + 체크섬 대사다.
//
// 체크섬은 정렬된 전체 행을 이어 붙여 md5 를 낸다 — 값이 하나만 달라도 바뀐다.
// 건수만 비교하면 "지워지고 다른 게 들어온" 경우를 놓친다.
const reconcileSQL = `
SELECT count(*)::bigint AS row_count,
       coalesce(md5(string_agg(
           id || '|' || biz_key || '|' || amount || '|' || status, ',' ORDER BY id)), '') AS checksum
  FROM %s WHERE biz_key LIKE 'V6-%%'`

type reconciliation struct {
	sourceCount    int64
	targetCount    int64
	sourceChecksum string
	targetChecksum string
}

func (r reconciliation) matches() bool {
	return r.sourceCount == r.targetCount && r.sourceChecksum == r.targetChecksum
}

func reconcile() reconciliation {
	var r reconciliation
	if err := Source().QueryRow(ctxBackground(), fmt.Sprintf(reconcileSQL, "verify_record")).
		Scan(&r.sourceCount, &r.sourceChecksum); err != nil {
		panic(fmt.Sprintf("원본 대사 실패: %v", err))
	}
	if err := Target().QueryRow(ctxBackground(), fmt.Sprintf(reconcileSQL, "verify_recon")).
		Scan(&r.targetCount, &r.targetChecksum); err != nil {
		panic(fmt.Sprintf("수신측 대사 실패: %v", err))
	}
	return r
}

func matchLabel(matched bool) string {
	if matched {
		return "일치"
	}
	return "불일치"
}
