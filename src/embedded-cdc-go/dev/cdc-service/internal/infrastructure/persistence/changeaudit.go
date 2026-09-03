package persistence

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/embedded-cdc-go/cdc-service/internal/domain/model"
	"github.com/embedded-cdc-go/cdc-service/internal/domain/port"
)

// ChangeAuditRepository 는 필드 단위 변경 이력을 cdc_change_audit 에 남긴다.
//
// 적용과 같은 트랜잭션에서 돈다(호출자가 이미 트랜잭션을 열어 둔다) —
// 적용이 롤백되면 이력도 함께 롤백돼야 "반영했다는 기록" 이 실제 상태와 어긋나지 않는다.
//
// 어느 표를 남길지는 목록으로 받는다. 세 가지 형태다 (Java 판과 같다):
//
//	비움          끔
//	car,computer  일부
//	* 또는 all    전체 — 목록에 섞여 있어도 전체로 본다 ("car,*" 는 "*")
//
// 전역 on/off 가 아닌 이유는 비용이다: 이벤트당 INSERT 가 한 건 더 붙으므로
// 대량 UPDATE 구간의 처리량이 그만큼 깎인다. 감사가 필요한 표에만 켠다.
type ChangeAuditRepository struct {
	store   *Store
	all     bool
	tables  map[string]struct{}
	metrics port.PipelineMetrics
}

// AuditAll 은 전체 표를 뜻하는 설정값이다.
const AuditAll = "*"

func NewChangeAuditRepository(store *Store, tables []string, metrics port.PipelineMetrics) *ChangeAuditRepository {
	r := &ChangeAuditRepository{store: store, tables: make(map[string]struct{}, len(tables)), metrics: metrics}
	for _, t := range tables {
		t = strings.TrimSpace(t)
		switch {
		case t == "":
			continue
		case t == AuditAll || strings.EqualFold(t, "all"):
			r.all = true
		default:
			r.tables[t] = struct{}{}
		}
	}
	return r
}

var _ port.ChangeAuditStore = (*ChangeAuditRepository)(nil)

// Enabled 는 감사를 켠 표가 하나라도 있는지다. 호출자가 diff 계산 자체를 건너뛸 때 쓴다.
func (r *ChangeAuditRepository) Enabled() bool { return r.all || len(r.tables) > 0 }

// Audits 는 이 표의 UPDATE 를 남기는지다. 전체/일부/끔 판정은 이 메서드 하나로 한다 —
// 호출자가 세 경우를 각각 따지게 두면 언젠가 하나를 빠뜨린다.
func (r *ChangeAuditRepository) Audits(table string) bool {
	if r.all {
		return true
	}
	_, ok := r.tables[table]
	return ok
}

func (r *ChangeAuditRepository) Record(
	ctx context.Context, pipeline string, event model.ChangeEvent, diff model.FieldDiff,
) error {
	if !r.Audits(event.Table) {
		return nil
	}

	_, err := r.store.exec(ctx).Exec(ctx, `
INSERT INTO cdc_change_audit (
    pipeline, source_table, row_key, source_lsn, source_ts,
    identifiable, changed_fields, unreadable_fields, before_values, after_values)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		pipeline,
		event.Table,
		rowKeyOf(event),
		int64(event.LSN),
		sourceTimestamp(event),
		diff.Identifiable,
		strings.Join(diff.Changed, ","),
		nullableList(diff.Unreadable),
		subsetJSON(event.Before, diff.Changed),
		subsetJSON(event.After, diff.Changed))
	if err != nil {
		return err
	}

	// 지표는 건수까지만. 필드명을 레이블로 올리면 카디널리티가 터진다.
	r.metrics.ChangeAudited(event.Table)
	return nil
}

// rowKeyOf 는 어느 행인지 짚기 위한 키다. 이 파이프라인의 대상 표는 모두 단일 PK(id)를 쓴다.
// 복합 키 표가 들어오면 여기서 조합해야 한다 — 그때까지는 없는 규칙을 만들지 않는다.
func rowKeyOf(event model.ChangeEvent) string {
	row := event.After
	if row == nil {
		row = event.Before
	}
	if row == nil {
		return "?"
	}
	if id, ok := row.Values()["id"]; ok && id != nil {
		return *id
	}
	return "?"
}

// sourceTimestamp 는 원천이 이 변경을 보낸 시각이다.
// Debezium 판의 source.ts_ms 자리이지만 여기서는 WAL 송신 시각(MessageTime)이다 —
// 커밋 시각이 아니라는 차이가 지연 해석에 그대로 들어간다(V1 메모 참고).
func sourceTimestamp(event model.ChangeEvent) time.Time {
	if event.SourceTsMs > 0 {
		return time.UnixMilli(event.SourceTsMs).UTC()
	}
	return time.Now().UTC()
}

func nullableList(values []string) *string {
	if len(values) == 0 {
		return nil
	}
	joined := strings.Join(values, ",")
	return &joined
}

// subsetJSON 은 바뀐 필드만 골라 JSON 으로 만든다.
// 행 전체를 남기면 이 표가 원본보다 빨리 커진다.
func subsetJSON(row *model.RowData, columns []string) *string {
	if row == nil || len(columns) == 0 {
		return nil
	}
	values := row.Values()

	// map 을 그대로 마셜링하면 키 순서가 Go 의 정렬 규칙을 타므로, 컬럼 순서를
	// 직접 쓴다 — 사람이 표에서 읽는 값이라 before/after 의 키 순서가 같아야 한다.
	var b strings.Builder
	b.WriteByte('{')
	for i, column := range columns {
		if i > 0 {
			b.WriteByte(',')
		}
		key, _ := json.Marshal(column)
		b.Write(key)
		b.WriteByte(':')
		if v, ok := values[column]; ok && v != nil {
			value, _ := json.Marshal(*v)
			b.Write(value)
		} else {
			b.WriteString("null")
		}
	}
	b.WriteByte('}')

	out := b.String()
	return &out
}
