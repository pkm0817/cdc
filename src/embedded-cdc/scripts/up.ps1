# 전체 스택 기동: 네트워크 → source/target DB → 모니터링 → cdc-service
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

# docker 가 없으면 podman 으로 (podman compose 는 docker-compose 를 위임 호출한다)
$engine = if (Get-Command docker -ErrorAction SilentlyContinue) { "docker" } else { "podman" }

# compose 파일들이 공유하는 외부 네트워크
$networks = & $engine network ls --format "{{.Name}}"
if ($networks -notcontains "emb-cdc-net") {
    & $engine network create emb-cdc-net | Out-Null
    Write-Host "network emb-cdc-net created"
}

& $engine compose -f "$root/infra/db/docker-compose.source.yml" up -d
& $engine compose -f "$root/infra/db/docker-compose.target.yml" up -d
& $engine compose -f "$root/infra/monitoring/docker-compose.monitoring.yml" up -d
& $engine compose -f "$root/dev/docker-compose.app.yml" up -d --build

Write-Host ""
Write-Host "── 기동 완료 ─────────────────────────────────────"
Write-Host "  source DB   : localhost:56432 (sourcedb / postgres:postgres)"
Write-Host "  target DB   : localhost:56433 (targetdb / postgres:postgres)"
Write-Host "  cdc-service : http://localhost:56080/actuator/health"
Write-Host "  Prometheus  : http://localhost:56090"
Write-Host "  Grafana     : http://localhost:56300  (admin/admin)"
