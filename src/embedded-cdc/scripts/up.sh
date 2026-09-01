#!/usr/bin/env bash
# 전체 스택 기동: 네트워크 → source/target DB → 모니터링 → cdc-service
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"

# docker 가 없으면 podman 으로 (podman compose 는 docker-compose 를 위임 호출한다)
engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

$engine network inspect emb-cdc-net >/dev/null 2>&1 || $engine network create emb-cdc-net

$engine compose -f "$root/infra/db/docker-compose.source.yml" up -d
$engine compose -f "$root/infra/db/docker-compose.target.yml" up -d
$engine compose -f "$root/infra/monitoring/docker-compose.monitoring.yml" up -d
$engine compose -f "$root/dev/docker-compose.app.yml" up -d --build

cat <<EOF

── 기동 완료 ─────────────────────────────────────
  source DB   : localhost:56432 (sourcedb / postgres:postgres)
  target DB   : localhost:56433 (targetdb / postgres:postgres)
  cdc-service : http://localhost:56080/actuator/health
  Prometheus  : http://localhost:56090
  Grafana     : http://localhost:56300  (admin/admin)
EOF
