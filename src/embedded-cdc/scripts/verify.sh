#!/usr/bin/env bash
# 캡처 신뢰성 검증 테스트(V1~V6) 실행.
# 기동 중인 source/target PostgreSQL 에 붙어 verify_* 전용 테이블·publication·슬롯으로만 돈다.
# 운영 테이블(car, computer)과 embedded_cdc_slot 은 건드리지 않는다.
#   ./verify.sh            전체
#   ./verify.sh 'V3*'      특정 시나리오만
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
app="$root/dev/cdc-service"

source_url="${CDC_VERIFY_SOURCE_URL:-jdbc:postgresql://localhost:56432/sourcedb}"
target_url="${CDC_VERIFY_TARGET_URL:-jdbc:postgresql://localhost:56433/targetdb}"

engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

# 테스트는 실행 중인 DB 를 전제로 한다. 안 떠 있으면 gradle 이 한참 뒤에야 실패한다
running="$($engine ps --format '{{.Names}}')"
for c in emb-cdc-source-pg emb-cdc-target-pg; do
  grep -qx "$c" <<<"$running" || { echo "$c 가 떠 있지 않다. 먼저 ./scripts/up.sh 를 실행할 것" >&2; exit 1; }
done

args=(test --console=plain
      "-Dcdc.verify.source.url=$source_url"
      "-Dcdc.verify.target.url=$target_url")
[ $# -gt 0 ] && args+=(--tests "$1")

(cd "$app" && ./gradlew "${args[@]}")

echo
echo "── 검증 산출물 ───────────────────────────────────"
echo "  계측 리포트 : $app/build/verification/results.md"
echo "  테스트 리포트: $app/build/reports/tests/test/index.html"
