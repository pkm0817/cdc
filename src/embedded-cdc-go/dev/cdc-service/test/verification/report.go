package verification

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Report 는 검증 계측치 수집기다.
//
// 테스트가 통과했는지(assert)와 별개로 "무엇이 얼마였는지"를 남긴다.
// 검증 보고서의 값은 전부 여기서 나오며, 사람이 나중에 손으로 옮겨 적지 않는다.
var Report = &report{}

type report struct {
	mu    sync.Mutex
	lines []string
}

// Section 은 항목 제목이다. 각 시나리오가 시작할 때 한 번 부른다.
func (r *report) Section(title string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, "", "## "+title)
	fmt.Println("[검증] " + title)
}

// Metric 은 계측치 한 줄이다. 이름과 값, 그리고 판정 근거를 함께 남긴다.
func (r *report) Metric(item, name string, value any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	text := fmt.Sprintf("%v", value)
	r.lines = append(r.lines, fmt.Sprintf("- **%s** · %s: `%s`", item, name, text))
	fmt.Printf("[%s] %s = %s\n", item, name, text)
}

// Note 는 수치가 아닌 관측된 사실이다. 통과/실패와 무관하게 기록한다.
func (r *report) Note(item, observation string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, fmt.Sprintf("- **%s** · %s", item, observation))
	fmt.Printf("[%s] %s\n", item, observation)
}

// Flush 는 모아 둔 계측치를 파일로 쓴다. TestMain 이 끝날 때 부른다.
func (r *report) Flush() {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := env("CDC_VERIFY_REPORT", filepath.Join("build", "verification", "results.md"))
	if dir := filepath.Dir(out); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "[검증] 계측치 기록 실패: %v\n", err)
			return
		}
	}

	body := "# CDC 캡처 신뢰성 검증 계측치 (go-pq-cdc)\n" +
		strings.Join(r.lines, "\n") + "\n"
	if err := os.WriteFile(out, []byte(body), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "[검증] 계측치 기록 실패: %v\n", err)
		return
	}
	abs, _ := filepath.Abs(out)
	fmt.Println("[검증] 계측치 기록: " + abs)
}
