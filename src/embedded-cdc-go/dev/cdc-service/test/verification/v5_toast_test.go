package verification

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
)

// V5. 이벤트가 비어서 오는 경우 (TOAST)
//
// PostgreSQL 은 큰 값을 행 밖(TOAST)에 저장하고, UPDATE 에서 그 컬럼이 바뀌지 않았으면
// WAL 에 새 값을 싣지 않는다. 관심 필드가 여기 걸리면 "값이 안 바뀐 것"과
// "값을 못 받은 것"을 구분할 수 없다.
//
// ── 값이 빠졌을 때의 모양이 Debezium 판과 다르다 ─────────────────────────────
// Debezium 은 그 자리를 "__debezium_unavailable_value" 라는 자리표시자로 채운다.
// go-pq-cdc 는 컬럼을 아예 싣지 않는다 — 우리 RowData 에서 "키가 없는" 상태가 된다.
// 그래서 판정 기준이 "자리표시자인가"가 아니라 "컬럼이 있는가"다.
// (RowData 의 값 타입이 *string 인 이유가 여기 있다 — NULL 과 부재를 구분해야 한다)
//
// 그리고 FULL 에서는 라이브러리가 old 이미지의 값으로 그 자리를 메워 준다.
// Debezium 은 before/after 를 그대로 주고 복원은 응용의 몫이었는데, 여기서는
// after 만 봐도 현재 값을 알 수 있다.
//
// 통과 기준
//   - 관심 필드가 모든 이벤트에서 항상 판독 가능한 설정을 찾아낸다
func TestV5ToastedField(t *testing.T) {
	const slotName = "verify_v5_slot"
	const table = "verify_toast"
	const publicationName = "verify_toast_pub"
	// TOAST 임계(약 2KB)를 확실히 넘기는 크기
	const payloadSize = 200_000

	Report.Section("V5. TOAST 로 값이 빠지는 경우")

	OnSource(`
CREATE TABLE IF NOT EXISTS verify_toast (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT      NOT NULL,
    big_payload TEXT      NOT NULL
)`)
	// 압축으로 인라인에 들어가 버리면 TOAST 상황이 재현되지 않는다.
	// EXTERNAL 은 압축 없이 무조건 행 밖에 저장하게 만든다.
	OnSource("ALTER TABLE verify_toast ALTER COLUMN big_payload SET STORAGE EXTERNAL")
	OnSource("ALTER TABLE verify_toast REPLICA IDENTITY DEFAULT")
	OnSource("TRUNCATE TABLE verify_toast RESTART IDENTITY")
	RecreatePublication(publicationName, table)

	DropSlotQuietly(slotName)
	harness, err := StartHarness(HarnessOptions{
		Slot:        slotName,
		Publication: publicationName,
		Tables:      []string{table},
		AutoAck:     true,
	})
	if err != nil {
		t.Fatalf("하니스 기동 실패: %v", err)
	}
	t.Cleanup(func() {
		harness.Stop()
		DropSlotQuietly(slotName)
	})

	t.Run("REPLICA IDENTITY DEFAULT 에서는 바뀌지 않은 대용량 필드가 빠져서 온다", func(t *testing.T) {
		harness.Drain()
		insertToastRow(t, "V5-DEFAULT", payload("A", payloadSize))

		captured, ok := harness.Poll(15 * time.Second)
		if !ok {
			t.Fatal("INSERT 이벤트가 도착해야 한다")
		}
		if captured.Event.Op != model.Create {
			t.Fatalf("op 가 c 여야 한다: %s", captured.Event.Op.Code())
		}
		// INSERT 에는 전체 값이 실린다 — 문제는 UPDATE 다
		if v, ok := Text(captured.Event.After, "big_payload"); !ok || len(v) != payloadSize {
			t.Fatalf("INSERT 에서는 대용량 필드도 온전히 와야 한다: 길이 %d", len(v))
		}
		Report.Note("V5", fmt.Sprintf("INSERT 이벤트에는 대용량 필드가 온전히 실린다 (%d bytes)", payloadSize))

		// big_payload 는 건드리지 않고 name 만 바꾼다
		OnSource("UPDATE verify_toast SET name = 'V5-DEFAULT-CHANGED' WHERE name = 'V5-DEFAULT'")
		updated, ok := harness.Poll(15 * time.Second)
		if !ok {
			t.Fatal("UPDATE 이벤트가 도착해야 한다")
		}

		value, present := updated.Event.After.Values()["big_payload"]
		absent := !present
		isNull := present && value == nil
		readable := present && value != nil && len(*value) == payloadSize

		Report.Metric("V5", "DEFAULT · after.big_payload 컬럼 자체가 없음", absent)
		Report.Metric("V5", "DEFAULT · after.big_payload 가 NULL", isNull)
		Report.Metric("V5", "DEFAULT · 실제 값 판독 가능", readable)
		Report.Metric("V5", "DEFAULT · before 존재", updated.Event.Before != nil)

		if readable {
			t.Fatal("DEFAULT 에서는 바뀌지 않은 TOAST 값을 읽을 수 없어야 한다 — " +
				"이것이 이 항목의 문제 상황이다")
		}
		Report.Note("V5", "DEFAULT 에서 관심 필드가 TOAST 대상이면 UPDATE 이벤트만으로는 현재 값을 알 수 없다. "+
			"Debezium 처럼 자리표시자가 오는 것이 아니라 컬럼 자체가 실리지 않는다")
	})

	t.Run("REPLICA IDENTITY FULL 로 바꾸면 값을 복원할 수 있다", func(t *testing.T) {
		OnSource("ALTER TABLE verify_toast REPLICA IDENTITY FULL")
		Report.Note("V5", "REPLICA IDENTITY FULL 로 변경 후 재측정")
		time.Sleep(time.Second)
		harness.Drain()

		insertToastRow(t, "V5-FULL", payload("B", payloadSize))
		if _, ok := harness.Poll(15 * time.Second); !ok {
			t.Fatal("선행 INSERT 이벤트가 도착해야 한다")
		}

		OnSource("UPDATE verify_toast SET name = 'V5-FULL-CHANGED' WHERE name = 'V5-FULL'")
		updated, ok := harness.Poll(15 * time.Second)
		if !ok {
			t.Fatal("UPDATE 이벤트가 도착해야 한다")
		}

		afterValue, afterOK := Text(updated.Event.After, "big_payload")
		beforeValue, beforeOK := Text(updated.Event.Before, "big_payload")
		afterReadable := afterOK && len(afterValue) == payloadSize
		beforeReadable := beforeOK && len(beforeValue) == payloadSize

		Report.Metric("V5", "FULL · after 판독 가능", afterReadable)
		Report.Metric("V5", "FULL · before 판독 가능", beforeReadable)
		Report.Metric("V5", "FULL · after 값 길이", len(afterValue))
		Report.Metric("V5", "FULL · before 값 길이", len(beforeValue))

		// 관심 필드를 "항상 판독 가능"하게 만들 수 있는지가 통과 기준이다.
		// after 든 before 든 한쪽에서 읽을 수 있으면 현재 값을 복원할 수 있다
		// (UPDATE 가 그 필드를 건드리지 않았으므로 before == 현재 값).
		recoverable := afterReadable || beforeReadable
		Report.Metric("V5", "FULL · 현재 값 복원 가능", recoverable)

		if !recoverable {
			Report.Note("V5", "경고: FULL 로도 TOAST 값이 오지 않는다. 관심 필드가 대용량이면 "+
				"이벤트만으로 동기화할 수 없고 원본 재조회가 필요하다")
			t.Fatal("FULL 에서 현재 값을 복원할 수 없다")
		}

		if afterReadable {
			Report.Note("V5", "FULL 에서는 after 로 바로 읽힌다 — go-pq-cdc 가 TOAST 로 빠진 컬럼을 "+
				"old 이미지 값으로 메워 준다. 응용에서 before 를 뒤질 필요가 없다")
		} else {
			Report.Note("V5", "FULL 에서는 before 로 현재 값을 복원할 수 있다")
		}
	})
}

func insertToastRow(t *testing.T, name, value string) {
	t.Helper()
	if _, err := Source().Exec(context.Background(),
		"INSERT INTO verify_toast (name, big_payload) VALUES ($1, $2)", name, value); err != nil {
		t.Fatalf("TOAST 행 삽입 실패: %v", err)
	}
}

// payload 는 압축이 잘 되지 않도록 문자를 섞어 만든다.
// 잘 압축되면 인라인에 들어가 TOAST 가 안 된다.
func payload(seed string, size int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	k := int32(7)
	for _, r := range seed {
		k = k*31 + r
	}
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		k = k*1103515245 + 12345
		idx := int(k>>16) % len(alphabet)
		if idx < 0 {
			idx = -idx
		}
		buf[i] = alphabet[idx]
	}
	return string(buf)
}
