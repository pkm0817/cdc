#!/usr/bin/env sh
# Kafka Connect 가 뜬 뒤 Debezium PostgreSQL 커넥터를 등록한다 (WSL/Linux 용).
# 사용: sh scripts/register-connector.sh
set -e

CONNECT="${CONNECT:-http://localhost:8084}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Kafka Connect 기동 대기 중... ($CONNECT)"
i=0
while [ "$i" -lt 60 ]; do
    if curl -sf "$CONNECT/connectors" >/dev/null 2>&1; then
        echo "Kafka Connect ready"
        break
    fi
    i=$((i + 1))
    sleep 3
done

echo "커넥터 등록 중..."
code=$(curl -s -o /tmp/connect-resp.json -w '%{http_code}' \
    -X POST "$CONNECT/connectors" \
    -H 'Content-Type: application/json' \
    -d @"$ROOT/connector/member-pg-connector.json")

case "$code" in
    201) echo "등록 완료" ;;
    409) echo "이미 등록되어 있습니다" ;;
    *)   echo "등록 실패 (HTTP $code)"; cat /tmp/connect-resp.json; exit 1 ;;
esac

sleep 6
echo "=== status ==="
curl -s "$CONNECT/connectors/member-pg-connector/status"
echo ""
