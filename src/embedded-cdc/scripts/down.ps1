# 전체 스택 정지. -Wipe 를 주면 볼륨(데이터·offset)까지 삭제
param([switch]$Wipe)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

$engine = if (Get-Command docker -ErrorAction SilentlyContinue) { "docker" } else { "podman" }

$flag = @()
if ($Wipe) { $flag = @("-v") }

# 앱을 먼저 내린다 — DB 를 먼저 내리면 replication slot 이 남은 채로 재접속을 반복한다
& $engine compose -f "$root/dev/docker-compose.app.yml" down @flag
& $engine compose -f "$root/infra/monitoring/docker-compose.monitoring.yml" down @flag
& $engine compose -f "$root/infra/db/docker-compose.source.yml" down @flag
& $engine compose -f "$root/infra/db/docker-compose.target.yml" down @flag

if ($Wipe) {
    Write-Host "볼륨까지 삭제됨 — 다음 기동 시 스키마 초기화 + snapshot 재실행"
} else {
    Write-Host "정지 완료 (데이터 유지). 완전 초기화는 ./scripts/down.ps1 -Wipe"
}
