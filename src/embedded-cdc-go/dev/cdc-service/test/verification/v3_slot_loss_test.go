package verification

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/embedded-cdc-go/cdc-service/internal/infrastructure/cdc"
)

// V3. 캡처 연결고리가 끊기는 경우 — 복구 불가능 구간을 탐지하는가
//
// 슬롯이 사라지면 그 사이 WAL 은 PostgreSQL 이 지워 버릴 수 있고, 되받을 방법이 없다.
// 그러므로 "복구"가 아니라 "탐지"가 통과 기준이다. 탐지하지 못하면 조용히 어긋난 채로 운영된다.
//
// ── Debezium 판과 결과가 다른 항목이다 ───────────────────────────────────────
// Debezium 은 오프셋 파일에 "여기서부터 읽어야 한다"가 적혀 있어서, 슬롯이 사라지면
// 그 지점을 더 이상 읽을 수 없다는 사실을 스스로 알아채고 기동을 거부했다.
// go-pq-cdc 에는 오프셋 파일이 없다 — 진행 지점이 슬롯 자체에만 있으므로,
// 슬롯이 사라지면 "어디까지 읽었는지"라는 정보도 함께 사라진다.
// 라이브러리 입장에서는 최초 기동과 구분이 되지 않아, 새 슬롯을 만들고 조용히 이어서 돈다.
//
// 그래서 이 스택에서는 SlotContinuityGuard(수신 측 체크포인트와 슬롯 대조)가
// 보조 수단이 아니라 유일한 탐지 수단이다. 이 시나리오가 그 사실을 확인한다.
//
// 통과 기준
//   - 유실이 실제로 발생한다 (막을 수 없는 것이므로 예상된 결과다)
//   - 라이브러리는 그것을 알려 주지 않는다 (이 판의 관측 결과)
//   - 체크포인트 대조 가드는 그것을 탐지한다
//   - 재동기화 절차로 정합을 복원할 수 있다
func TestV3SlotLoss(t *testing.T) {
	const slotName = "verify_v3_slot"
	const changesInGap = 100

	Report.Section("V3. 복제 슬롯 유실")
	CreateRecordFixture()
	TruncateRecords()
	DropSlotQuietly(slotName)
	t.Cleanup(func() { DropSlotQuietly(slotName) })

	var lastConfirmedLSN string

	t.Run("슬롯이 삭제되면 라이브러리는 조용히 새로 만들고 넘어간다", func(t *testing.T) {
		first, err := StartHarness(HarnessOptions{
			Slot:        slotName,
			Publication: RecordPublication,
			Tables:      []string{RecordTable},
			AutoAck:     true,
		})
		if err != nil {
			t.Fatalf("1차 하니스 기동 실패: %v", err)
		}

		InsertRecord("V3-BEFORE-1", "100.00", "NEW")
		if _, ok := first.Poll(10 * time.Second); !ok {
			t.Fatal("정상 캡처가 되어야 한다")
		}
		time.Sleep(2 * time.Second) // ack 가 슬롯에 닿을 시간

		lastConfirmedLSN = SlotConfirmedFlushLSN(slotName)
		Report.Metric("V3", "슬롯 삭제 직전 confirmed_flush_lsn", lastConfirmedLSN)

		first.Stop()

		// ── 연결고리 절단 ─────────────────────────────────────────────────
		DropSlotQuietly(slotName)
		if SlotExists(slotName) {
			t.Fatal("슬롯이 사라진 상태를 만들어야 한다")
		}
		Report.Note("V3", "복제 슬롯 강제 삭제 — DB 재기동이나 이중화 전환에서도 같은 상태가 된다")

		// ── 되받을 수 없는 구간의 변경 ────────────────────────────────────
		InsertRecordsInOneTransaction("V3-GAP", changesInGap)
		time.Sleep(time.Second)

		// ── 재기동 ────────────────────────────────────────────────────────
		second, err := StartHarness(HarnessOptions{
			Slot:        slotName,
			Publication: RecordPublication,
			Tables:      []string{RecordTable},
			AutoAck:     true,
		})
		if err != nil {
			// 라이브러리가 기동을 거부했다면 그것도 유효한 결과다 — 사실대로 남긴다.
			Report.Metric("V3", "재기동 결과", "라이브러리가 기동을 거부함: "+err.Error())
			Report.Note("V3", "라이브러리가 슬롯 유실을 스스로 탐지했다 — 가드 없이도 알아챌 수 있다")
			return
		}
		defer second.Stop()

		received := second.Collect(changesInGap, 20*time.Second)
		gapKeys := 0
		for _, c := range received {
			if key, ok := Text(c.Event.After, "biz_key"); ok && strings.HasPrefix(key, "V3-GAP-") {
				gapKeys++
			}
		}
		lost := changesInGap - gapKeys

		Report.Metric("V3", "끊긴 구간 변경 건수", changesInGap)
		Report.Metric("V3", "재기동 후 수신", fmt.Sprintf("%d / %d", gapKeys, changesInGap))
		Report.Metric("V3", "슬롯이 새로 만들어졌는가", SlotExists(slotName))
		Report.Metric("V3", "유실 건수", lost)

		// 슬롯이 사라진 구간을 되받을 방법은 없다. 유실 자체는 막을 수 없는 것이므로
		// 이 항목의 통과 기준은 "유실이 나지 않는 것"이 아니라 "유실을 알아채는 것"이다.
		if lost != changesInGap {
			t.Fatalf("슬롯이 사라진 구간은 되받을 수 없다 — 유실이 %d건이어야 하는데 %d건이다",
				changesInGap, lost)
		}

		Report.Note("V3", "go-pq-cdc 는 오프셋 파일이 없어 슬롯 유실을 스스로 알아채지 못한다. "+
			"createIfNotExists 설정대로 새 슬롯을 만들고 조용히 이어서 돈다 — "+
			"Debezium 판이 기동을 거부하던 자리다. 탐지는 전적으로 SlotContinuityGuard 몫이다")
	})

	t.Run("체크포인트와 슬롯을 대조하면 유실을 기동 시점에 탐지할 수 있다", func(t *testing.T) {
		// 운영 가드와 같은 규칙: 처리 이력이 있는데 슬롯이 없거나,
		// restart_lsn 이 마지막 처리 지점보다 앞서 있으면 그 사이는 되받을 수 없다.
		slotPresent := SlotExists(slotName)
		currentRestart := SlotRestartLSN(slotName)

		gapDetected := !slotPresent || currentRestart == ""
		if !gapDetected {
			restart, err := cdc.ParseLSN(currentRestart)
			if err != nil {
				t.Fatalf("restart_lsn 을 읽지 못했다: %v", err)
			}
			last, err := cdc.ParseLSN(lastConfirmedLSN)
			if err != nil {
				t.Fatalf("직전 처리 LSN 을 읽지 못했다: %v", err)
			}
			gapDetected = restart > last
		}

		Report.Metric("V3", "직전 처리 LSN", lastConfirmedLSN)
		Report.Metric("V3", "현재 슬롯 restart_lsn", currentRestart)
		Report.Metric("V3", "가드가 유실을 탐지했는가", gapDetected)

		if !gapDetected {
			t.Fatal("슬롯의 restart_lsn 이 마지막 처리 지점보다 앞서 있으면 그 사이는 영영 못 받는다 — " +
				"가드가 이를 탐지해야 한다")
		}

		Report.Note("V3", "운영 코드의 SlotContinuityGuard 가 기동 시 같은 대조를 한다. "+
			"cdc.fail-on-capture-gap 이 켜져 있으면 기동을 거부하고 재동기화를 요구한다")
	})

	t.Run("재동기화(전량 재적재)로 정합을 복원할 수 있다", func(t *testing.T) {
		// 끊긴 구간은 WAL 로 못 받으므로 원본을 다시 읽는 수밖에 없다.
		// go-pq-cdc 의 snapshot 이 하는 일과 같은 것을 검증 테이블에서 재현한다.
		OnTarget(`
CREATE TABLE IF NOT EXISTS verify_record_replica (
    id         BIGINT        PRIMARY KEY,
    biz_key    TEXT          NOT NULL,
    amount     NUMERIC(14,2) NOT NULL,
    status     TEXT          NOT NULL
)`)
		OnTarget("TRUNCATE TABLE verify_record_replica")

		type record struct {
			id     int64
			bizKey string
			amount string
			status string
		}
		rows, err := Source().Query(context.Background(),
			"SELECT id, biz_key, amount::text, status FROM verify_record ORDER BY id")
		if err != nil {
			t.Fatalf("원본 조회 실패: %v", err)
		}
		var all []record
		for rows.Next() {
			var r record
			if err := rows.Scan(&r.id, &r.bizKey, &r.amount, &r.status); err != nil {
				rows.Close()
				t.Fatalf("원본 판독 실패: %v", err)
			}
			all = append(all, r)
		}
		rows.Close()
		if len(all) == 0 {
			t.Fatal("원본에 행이 있어야 이 항목이 성립한다")
		}

		// 유실 상태를 흉내 낸다: 절반만 들어가 있는 상태
		half := len(all) / 2
		for _, r := range all[:half] {
			UpdateOnTarget(
				"INSERT INTO verify_record_replica (id, biz_key, amount, status) VALUES ($1, $2, $3::numeric, $4)",
				r.id, r.bizKey, r.amount, r.status)
		}

		beforeResync := CountOnTarget("SELECT count(*) FROM verify_record_replica")
		if beforeResync >= int64(len(all)) {
			t.Fatalf("재동기화 전에는 어긋나 있어야 한다: %d / %d", beforeResync, len(all))
		}

		// ── 재동기화 절차: 전량을 멱등 UPSERT 로 다시 적재 ─────────────────
		for _, r := range all {
			UpdateOnTarget(`
INSERT INTO verify_record_replica (id, biz_key, amount, status)
VALUES ($1, $2, $3::numeric, $4)
ON CONFLICT (id) DO UPDATE SET
    biz_key = EXCLUDED.biz_key, amount = EXCLUDED.amount, status = EXCLUDED.status`,
				r.id, r.bizKey, r.amount, r.status)
		}

		afterResync := CountOnTarget("SELECT count(*) FROM verify_record_replica")
		Report.Metric("V3", "재동기화 전 수신측 건수", fmt.Sprintf("%d / %d", beforeResync, len(all)))
		Report.Metric("V3", "재동기화 후 수신측 건수", fmt.Sprintf("%d / %d", afterResync, len(all)))

		if afterResync != int64(len(all)) {
			t.Fatalf("전량 재적재로 정합이 복원되어야 한다: %d / %d", afterResync, len(all))
		}
		Report.Note("V3", "재동기화는 멱등 UPSERT 라 중복 적재가 안전하다. "+
			"다만 원본에서 사라진 행은 재적재로 지워지지 않으므로 삭제 대사가 따로 필요하다")
	})
}
