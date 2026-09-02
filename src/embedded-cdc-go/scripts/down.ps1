# 전체 스택 정지. -Wipe 를 주면 볼륨(데이터)까지 삭제
param([switch]$Wipe)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

$engine = if (Get-Command docker -ErrorAction SilentlyContinue) { "docker" } else { "podman" }

$flag = @()
if ($Wipe) { $flag = @("-v") }

# 루트 compose 가 스택 전체를 한 프로젝트로 잡고 있으므로 한 번에 내린다.
# compose 가 앱 → DB 순으로 의존성 역순 정지를 보장한다 — replication slot 이
# 남은 채 재접속을 반복하는 상황을 피하려면 이 순서여야 한다.
& $engine compose -f "$root/docker-compose.yml" down @flag

if ($Wipe) {
    Write-Host "볼륨까지 삭제됨 — 다음 기동 시 스키마 초기화 + snapshot 재실행"
} else {
    Write-Host "정지 완료 (데이터 유지). 완전 초기화는 ./scripts/down.ps1 -Wipe"
}
