#!/usr/bin/env bash
# 캡처 신뢰성 검증 테스트(V1~V6) 실행.
# 기동 중인 source/target PostgreSQL 에 붙어 verify_* 전용 테이블·publication·슬롯으로만 돈다.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
app="$root/dev/cdc-service"

run_filter=""
[ $# -gt 0 ] && run_filter="$1"   # 예: ./scripts/verify.sh V3

command -v go >/dev/null 2>&1 || { echo "go 를 찾지 못했다. Go 1.25 이상 필요" >&2; exit 1; }

engine=docker
command -v docker >/dev/null 2>&1 || engine=podman
for c in emb-cdc-go-source-pg emb-cdc-go-target-pg; do
  $engine ps --format '{{.Names}}' | grep -qx "$c" || {
    echo "$c 가 떠 있지 않다. 먼저 ./scripts/up.sh 를 실행할 것" >&2; exit 1; }
done

export CDC_VERIFY_SOURCE_URL="${CDC_VERIFY_SOURCE_URL:-postgres://postgres:postgres@localhost:57432/sourcedb?sslmode=disable}"
export CDC_VERIFY_TARGET_URL="${CDC_VERIFY_TARGET_URL:-postgres://postgres:postgres@localhost:57433/targetdb?sslmode=disable}"
export CDC_VERIFY_REPORT="${CDC_VERIFY_REPORT:-$app/build/verification/results.md}"

# -count=1 : 캐시된 결과를 재사용하지 않는다. 실제 DB 상태에 따라 결과가 달라지는 테스트다.
# -p 1     : 패키지를 순차 실행한다. 슬롯과 WAL 을 다투면 계측치가 뒤섞인다.
cd "$app"
if [ -n "$run_filter" ]; then
  go test ./test/verification/... -v -count=1 -p 1 -timeout 30m -run "$run_filter"
else
  go test ./test/verification/... -v -count=1 -p 1 -timeout 30m
fi
code=$?

echo
echo "── 검증 산출물 ───────────────────────────────────"
echo "  계측 리포트 : $CDC_VERIFY_REPORT"
exit $code
