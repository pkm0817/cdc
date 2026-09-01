#!/usr/bin/env bash
# CDC 동작 데모: source 에 INSERT/UPDATE/DELETE 를 넣고 target 반영을 확인
set -euo pipefail

engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

src() { $engine exec emb-cdc-source-pg psql -U postgres -d sourcedb -c "$1"; }
tgt() { $engine exec emb-cdc-target-pg psql -U postgres -d targetdb -c "$1"; }

echo "── 1. INSERT ──────────────────────────────────────"
src "INSERT INTO car (name, brand, price) VALUES ('Ioniq 5', 'Hyundai', 52000000.00);"
src "INSERT INTO computer (brand, model, cpu, ram_gb, price_usd) VALUES ('ASUS', 'ROG Zephyrus', 'Ryzen 9', 32, 2299.00);"

echo "── 2. UPDATE ──────────────────────────────────────"
src "UPDATE car SET price = price * 0.95, updated_at = now() WHERE name = 'Avante';"
src "UPDATE computer SET ram_gb = 48 WHERE model = 'ThinkPad X1';"

echo "── 3. DELETE ──────────────────────────────────────"
src "DELETE FROM car WHERE name = 'K5';"
src "DELETE FROM computer WHERE model = 'XPS 13';"

echo "── 4. 복제 대기 (3초) ─────────────────────────────"
sleep 3

echo "── 5. source 상태 ─────────────────────────────────"
src "SELECT id, name, brand, price FROM car ORDER BY id;"
src "SELECT id, brand, model, ram_gb, price_usd FROM computer ORDER BY id;"

echo "── 6. target 상태 ─────────────────────────────────"
tgt "SELECT id, name, brand, price FROM car ORDER BY id;"
tgt "SELECT id, full_name, spec, price_krw, deleted, source_lsn FROM computer ORDER BY id;"

echo "── 7. 행 수 정합성 ────────────────────────────────"
src "SELECT 'car' AS t, count(*) FROM car UNION ALL SELECT 'computer', count(*) FROM computer;"
tgt "SELECT 'car' AS t, count(*) FROM car UNION ALL SELECT 'computer(active)', count(*) FROM computer WHERE deleted = false;"
