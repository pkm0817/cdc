#!/usr/bin/env bash
# 전체 스택 정지. --wipe 를 주면 볼륨(데이터)까지 삭제
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"

engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

flag=""
[ "${1:-}" = "--wipe" ] && flag="-v"

# 루트 compose 가 스택 전체를 한 프로젝트로 잡고 있으므로 한 번에 내린다.
# compose 가 앱 → DB 순으로 의존성 역순 정지를 보장한다 — replication slot 이
# 남은 채 재접속을 반복하는 상황을 피하려면 이 순서여야 한다.
$engine compose -f "$root/docker-compose.yml" down $flag

if [ -n "$flag" ]; then
  echo "볼륨까지 삭제됨 — 다음 기동 시 스키마 초기화 + snapshot 재실행"
else
  echo "정지 완료 (데이터 유지). 완전 초기화는 ./scripts/down.sh --wipe"
fi
