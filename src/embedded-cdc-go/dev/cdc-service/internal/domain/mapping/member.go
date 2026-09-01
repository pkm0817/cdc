package mapping

import "github.com/embedded-cdc-go/cdc-service/internal/domain/model"

// Member 는 source 의 member 행을 target 의 member 로 옮긴다.
//
// grade_id 를 grade 조회 없이 그대로 싣는다는 점이 이 매퍼의 내용이다.
// 예컨대 grade.code 를 붙여 비정규화하려면 여기서 grade 를 조회해야 하는데,
// 그러면 매퍼가 순수 함수가 아니게 되고 grade 가 아직 안 왔을 때 실패한다.
// 그런 결합은 CDC 적재가 아니라 하류(뷰·배치)에서 푸는 편이 낫다.
func Member(row model.RowData, lsn uint64) (model.Member, error) {
	id, err := row.Int64("id")
	if err != nil {
		return model.Member{}, err
	}
	email, err := row.Text("email")
	if err != nil {
		return model.Member{}, err
	}
	name, err := row.Text("name")
	if err != nil {
		return model.Member{}, err
	}
	gradeID, err := row.Int64("grade_id")
	if err != nil {
		return model.Member{}, err
	}
	point, err := row.Int32("point")
	if err != nil {
		return model.Member{}, err
	}
	createdAt, err := row.Timestamp("created_at")
	if err != nil {
		return model.Member{}, err
	}
	updatedAt, err := row.Timestamp("updated_at")
	if err != nil {
		return model.Member{}, err
	}
	return model.Member{
		ID:        id,
		Email:     email,
		Name:      name,
		GradeID:   gradeID,
		Point:     point,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		SourceLSN: lsn,
	}, nil
}
