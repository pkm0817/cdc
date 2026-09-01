package model

// SourceTable 은 이 파이프라인이 동기화하는 source 테이블이다.
//
// 여기에 없는 테이블의 이벤트는 무시된다. publication 이 네 테이블만 담고 있으므로
// 평소에는 도달하지 않지만, publication 이 바뀌면 조용히 유실되는 대신 경고가 남도록
// 이름을 상수로 고정해 둔다.
type SourceTable string

const (
	TableCar      SourceTable = "car"
	TableComputer SourceTable = "computer"

	// grade 는 member 의 부모다. 선언 순서는 적용 순서와 무관하다 —
	// 순서는 이벤트의 LSN 이 정하고, BatchApplier 가 그 순서대로 부른다.
	TableGrade  SourceTable = "grade"
	TableMember SourceTable = "member"
)

var knownTables = map[string]SourceTable{
	string(TableCar):      TableCar,
	string(TableComputer): TableComputer,
	string(TableGrade):    TableGrade,
	string(TableMember):   TableMember,
}

func SourceTableFromName(name string) (SourceTable, bool) {
	t, ok := knownTables[name]
	return t, ok
}

func (t SourceTable) Name() string {
	return string(t)
}
