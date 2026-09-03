package model

import "sort"

// FieldDiff 는 한 UPDATE 에서 "무엇이 바뀌었는지" 의 판정 결과다.
//
// V1 의 통과 기준 절반이 이것이다 — 이벤트가 제때 오는 것만으로는 부족하고,
// 어떤 필드가 무엇에서 무엇으로 바뀌었는지 말할 수 있어야 한다.
//
// 판정은 before/after 비교 하나뿐이라 REPLICA IDENTITY FULL 이 전제다.
// DEFAULT 면 before 에 PK 만 실려 비교 자체가 성립하지 않는다 — 그 경우를
// "변경 없음" 으로 뭉개지 않고 Identifiable=false 로 구분해 돌려준다.
// 둘을 같은 값으로 돌려주면 "안 바뀐 것" 과 "모르는 것" 이 감사 로그에서 섞이고,
// 그러면 REPLICA IDENTITY 설정 사고를 사후에 찾을 수 없다.
//
// Debezium 판과 다른 점이 하나 있다. 그쪽은 값을 실을 수 없을 때
// __debezium_unavailable_value 자리표시자를 채워 넣지만, go-pq-cdc 는
// 컬럼 자체를 싣지 않는다. 그래서 "판정 못 한 컬럼" 의 신호가 자리표시자가 아니라
// 키의 부재다 — RowData 가 "없는 컬럼" 과 "NULL 인 컬럼" 을 구분해 두는 이유가 이것이다.
//
//	Identifiable 필드 단위 판정이 가능한 이벤트였는지 (before/after 가 둘 다 있는지)
//	Changed      실제로 값이 달라진 컬럼
//	Unreadable   한쪽 이미지에 실려 오지 않아 판정에서 제외한 컬럼 (TOAST · V5)
type FieldDiff struct {
	Identifiable bool
	Changed      []string
	Unreadable   []string
}

// FieldDiffBetween 은 before 와 after 를 견줘 바뀐 필드만 뽑는다.
// 한쪽이라도 없으면(INSERT/DELETE, 또는 REPLICA IDENTITY DEFAULT) 판정 불가로 돌려준다.
func FieldDiffBetween(before, after *RowData) FieldDiff {
	if before == nil || after == nil {
		return FieldDiff{}
	}

	b := before.Values()
	a := after.Values()

	columns := make(map[string]struct{}, len(b)+len(a))
	for c := range b {
		columns[c] = struct{}{}
	}
	for c := range a {
		columns[c] = struct{}{}
	}

	var changed, unreadable []string
	for column := range columns {
		bv, bok := b[column]
		av, aok := a[column]

		// 한쪽에만 실려 온 컬럼은 "바뀌었다" 가 아니라 "못 읽었다" 다.
		// 그냥 비교하면 안 건드린 대용량(TOAST) 컬럼이 매번 변경으로 잡혀
		// 감사 로그가 거짓말을 한다.
		if !bok || !aok {
			unreadable = append(unreadable, column)
			continue
		}
		if !sameValue(bv, av) {
			changed = append(changed, column)
		}
	}

	sort.Strings(changed)
	sort.Strings(unreadable)
	return FieldDiff{Identifiable: true, Changed: changed, Unreadable: unreadable}
}

func (d FieldDiff) HasChanges() bool { return len(d.Changed) > 0 }

func sameValue(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
