// Package cdc 는 go-pq-cdc 를 이 파이프라인에 붙이는 구동 어댑터다.
package cdc

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/jackc/pgx/v5/pgtype"
)

// Decode 는 go-pq-cdc 메시지를 도메인 ChangeEvent 로 바꾼다.
//
// 라이브러리 타입이 등장하는 유일한 곳이며, 여기서 막아야 도메인이 이벤트 포맷에 묶이지 않는다.
// 두 번째 값이 false 면 이 파이프라인의 관심 밖인 메시지(Relation, Begin/Commit, Truncate,
// 스냅샷 시작·종료 알림 등)라는 뜻이다 — 오류가 아니라 무시 대상이다.
//
// Debezium 과 다른 점이 둘 있고, 둘 다 아래 주석에 적어 둔다.
//   - op 코드가 없다. 메시지 Go 타입이 곧 종류라, 여기서 한 글자 코드로 되돌린다
//   - 커밋 시각이 없다. 있는 것은 WAL 송신 시각(MessageTime)이다
func Decode(msg any) (model.ChangeEvent, bool, error) {
	switch m := msg.(type) {
	case *format.Insert:
		after, err := toRowData(m.Decoded)
		if err != nil {
			return model.ChangeEvent{}, false, err
		}
		return model.ChangeEvent{
			Table:      m.TableName,
			Op:         model.Create,
			After:      after,
			LSN:        uint64(m.LSN),
			SourceTsMs: toMillis(m.MessageTime),
		}, true, nil

	case *format.Update:
		before, err := toRowData(m.OldDecoded)
		if err != nil {
			return model.ChangeEvent{}, false, err
		}
		after, err := toRowData(m.NewDecoded)
		if err != nil {
			return model.ChangeEvent{}, false, err
		}
		return model.ChangeEvent{
			Table:      m.TableName,
			Op:         model.Update,
			Before:     before,
			After:      after,
			LSN:        uint64(m.LSN),
			SourceTsMs: toMillis(m.MessageTime),
		}, true, nil

	case *format.Delete:
		before, err := toRowData(m.OldDecoded)
		if err != nil {
			return model.ChangeEvent{}, false, err
		}
		return model.ChangeEvent{
			Table:      m.TableName,
			Op:         model.Delete,
			Before:     before,
			LSN:        uint64(m.LSN),
			SourceTsMs: toMillis(m.MessageTime),
		}, true, nil

	case *format.Snapshot:
		// 스냅샷은 BEGIN / DATA / END 세 종류가 온다. 실제 행이 실린 것은 DATA 뿐이다.
		if m.EventType != format.SnapshotEventTypeData {
			return model.ChangeEvent{}, false, nil
		}
		after, err := toRowData(m.Data)
		if err != nil {
			return model.ChangeEvent{}, false, err
		}
		return model.ChangeEvent{
			Table: m.Table,
			Op:    model.SnapshotRead,
			After: after,
			LSN:   uint64(m.LSN),
			// 스냅샷 행에는 "언제 바뀌었는지" 가 없다. 0 을 넣어 지연 지표에서 빠지게 한다 —
			// 스냅샷 시각을 지연으로 세면 최초 기동 때마다 지연이 폭발한 것처럼 보인다.
			SourceTsMs: 0,
		}, true, nil

	default:
		return model.ChangeEvent{}, false, nil
	}
}

// toRowData 는 라이브러리가 디코딩한 값을 전부 문자열로 편다.
//
// 값을 문자열로 통일하는 이유는 RowData 주석에 있다 — 손실이 없고,
// DLQ 페이로드로 그대로 직렬화·복원된다.
//
// 값이 nil 이면 진짜 NULL 이고, 아예 없는 키는 "실려 오지 않은 컬럼"이다.
// REPLICA IDENTITY FULL 이 아닌 표에서 UPDATE 가 TOAST 컬럼을 건드리지 않으면
// 후자가 된다 — 두 경우를 구분해야 "값이 안 바뀐 것"과 "값을 못 받은 것"을 가릴 수 있다.
func toRowData(decoded map[string]any) (*model.RowData, error) {
	if decoded == nil {
		return nil, nil
	}
	values := make(map[string]*string, len(decoded))
	for column, raw := range decoded {
		text, err := toText(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: 컬럼 %s: %v", model.ErrBadData, column, err)
		}
		values[column] = text
	}
	row := model.NewRowData(values)
	return &row, nil
}

func toText(raw any) (*string, error) {
	if raw == nil {
		return nil, nil
	}
	var s string
	switch v := raw.(type) {
	case string:
		s = v
	case bool:
		s = strconv.FormatBool(v)
	case int16:
		s = strconv.FormatInt(int64(v), 10)
	case int32:
		s = strconv.FormatInt(int64(v), 10)
	case int64:
		s = strconv.FormatInt(v, 10)
	case float32:
		s = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		s = strconv.FormatFloat(v, 'f', -1, 64)
	case time.Time:
		// RowData.Timestamp 가 다시 읽을 수 있는 표기여야 한다.
		s = v.UTC().Format(time.RFC3339Nano)
	case pgtype.Numeric:
		// NUMERIC 은 여기서 십진 문자열로 되돌린다. float 을 거치면 금액이 어긋난다.
		value, err := v.Value()
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("NUMERIC 을 문자열로 얻지 못했다: %T", value)
		}
		s = text
	case []byte:
		s = string(v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	return &s, nil
}

func toMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}
