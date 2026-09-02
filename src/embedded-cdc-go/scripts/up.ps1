# 전체 스택 기동. 루트 docker-compose.yml 하나가 include 로 DB·모니터링·앱을 모두 끌어온다.
# 네트워크와 볼륨은 compose 가 만든다 — 미리 만들 필요 없다.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

# docker 가 없으면 podman 으로 (podman compose 는 docker-compose 를 위임 호출한다)
$engine = if (Get-Command docker -ErrorAction SilentlyContinue) { "docker" } else { "podman" }

# compose 파일을 하나씩 -f 로 올리지 않는다. 그렇게 하면 파일마다 프로젝트가 따로
# 잡혀 같은 container_name 을 두고 충돌하고, 볼륨도 두 벌이 생긴다.
& $engine compose -f "$root/docker-compose.yml" up -d --build

Write-Host ""
Write-Host "── 기동 완료 ─────────────────────────────────────"
Write-Host "  source DB   : localhost:57432 (sourcedb / postgres:postgres)"
Write-Host "  target DB   : localhost:57433 (targetdb / postgres:postgres)"
Write-Host "  cdc-service : http://localhost:57080/status  (지표는 /metrics)"
Write-Host "  Prometheus  : http://localhost:57090"
Write-Host "  Grafana     : http://localhost:57300  (admin/admin)"
