#!/usr/bin/env bash
# 전체 스택 기동: 네트워크 → source/target DB → 모니터링 → cdc-service
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"

# docker 가 없으면 podman 으로 (podman compose 는 docker-compose 를 위임 호출한다)
engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

$engine network inspect emb-cdc-go-net >/dev/null 2>&1 || $engine network create emb-cdc-go-net

$engine compose -f "$root/infra/db/docker-compose.source.yml" up -d
$engine compose -f "$root/infra/db/docker-compose.target.yml" up -d
$engine compose -f "$root/infra/monitoring/docker-compose.monitoring.yml" up -d
$engine compose -f "$root/dev/docker-compose.app.yml" up -d --build

cat <<EOS

── 기동 완료 ─────────────────────────────────────
  source DB   : localhost:57432 (sourcedb / postgres:postgres)
  target DB   : localhost:57433 (targetdb / postgres:postgres)
  cdc-service : http://localhost:57080/status  (지표는 /metrics)
  Prometheus  : http://localhost:57090
  Grafana     : http://localhost:57300  (admin/admin)
EOS
