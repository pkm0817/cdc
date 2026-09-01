package persistence

import (
	"context"
	"fmt"
)

// softDelete 는 computer·grade·member 가 공유하는 삭제 문장이다.
//
// 테이블 이름을 문자열로 끼워 넣지만 값은 이 파일 안의 상수 호출자에서만 온다 —
// 이벤트에 실려 온 이름이 여기로 들어오는 경로는 없다.
func softDelete(ctx context.Context, store *Store, table string, id int64, lsn uint64) (int64, error) {
	lsnValue, err := lsnToInt64(lsn)
	if err != nil {
		return 0, err
	}
	sql := fmt.Sprintf(`
UPDATE %s
   SET deleted = true, source_lsn = $2, synced_at = now()
 WHERE id = $1 AND source_lsn < $2`, table)

	tag, err := store.exec(ctx).Exec(ctx, sql, id, lsnValue)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
