package verification

import (
	"os"
	"testing"
)

// TestMain 은 계측치 리포트의 수명을 잡는다.
//
// 시나리오는 하나의 패키지 안에서 순차로 돈다(t.Parallel 을 쓰지 않는다).
// 슬롯과 WAL 은 원천 DB 전체가 공유하는 자원이라, 동시에 돌리면 서로의 계측치를 흔든다.
func TestMain(m *testing.M) {
	code := m.Run()
	Report.Flush()
	os.Exit(code)
}
