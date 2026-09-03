#!/usr/bin/env bash
# 캡처 신뢰성 검증 테스트(V1~V6) 실행.
# 기동 중인 source/target PostgreSQL 에 붙어 verify_* 전용 테이블·publication·슬롯으로만 돈다.
# 운영 테이블(car, computer)과 embedded_cdc_slot 은 건드리지 않는다.
#
# 예외가 하나 있다 — V4-b(DLQ 재처리 중복 유입)는 운영 테이블과 기동 중인 cdc-service 를
# 그대로 쓴다. 검증 대상이 각 테이블의 저장소 가드와 운영 DLQ 재처리기 자체라서
# 복제본으로는 답이 나오지 않는다. 넣은 행은 시나리오 끝에서 원천·수신 양쪽에서 지운다.
#
#   ./verify.sh                   전체
#   ./verify.sh 'V3*'             특정 시나리오만
#   ./verify.sh 'V4*'             V4 (재기동 중복 + DLQ 재처리 중복)
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
app="$root/dev/cdc-service"

source_url="${CDC_VERIFY_SOURCE_URL:-jdbc:postgresql://localhost:56432/sourcedb}"
target_url="${CDC_VERIFY_TARGET_URL:-jdbc:postgresql://localhost:56433/targetdb}"

engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

# 테스트는 실행 중인 DB 를 전제로 한다. 안 떠 있으면 gradle 이 한참 뒤에야 실패한다
running="$($engine ps --format '{{.Names}}')"
# cdc-service 까지 확인하는 이유는 V4-b 때문이다. 서비스가 없으면 "가드가 막았다"와
# "아무 일도 일어나지 않았다"가 같은 결과로 보여 통과가 거짓이 된다.
for c in emb-cdc-source-pg emb-cdc-target-pg emb-cdc-service; do
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
