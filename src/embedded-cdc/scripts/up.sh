#!/usr/bin/env bash
# 전체 스택 기동. 루트 docker-compose.yml 하나가 include 로 DB·모니터링·앱을 모두 끌어온다.
# 네트워크와 볼륨은 compose 가 만든다 — 미리 만들 필요 없다.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"

# docker 가 없으면 podman 으로 (podman compose 는 docker-compose 를 위임 호출한다)
engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

# compose 파일을 하나씩 -f 로 올리지 않는다. 그렇게 하면 파일마다 프로젝트가 따로
# 잡혀 같은 container_name 을 두고 충돌하고, 볼륨도 두 벌이 생긴다.
$engine compose -f "$root/docker-compose.yml" up -d --build

cat <<EOS

── 기동 완료 ─────────────────────────────────────
  source DB   : localhost:56432 (sourcedb / postgres:postgres)
  target DB   : localhost:56433 (targetdb / postgres:postgres)
  cdc-service : http://localhost:56080/actuator/health
  Prometheus  : http://localhost:56090
  Grafana     : http://localhost:56300  (admin/admin)
EOS
