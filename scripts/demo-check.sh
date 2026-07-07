#!/usr/bin/env bash
# 원커맨드 데모 검증 (AC10-4)
# compose up부터 seller-dashboard 뷰가 실데이터를 반환할 때까지 5분 타임아웃으로 자동 확인.
# 요구: docker, docker compose, curl, jq.
set -euo pipefail

cd "$(dirname "$0")/.."

REST="http://localhost:8080"
TIMEOUT="${TIMEOUT:-300}"   # 5분

log() { echo "[demo-check] $*"; }

log "compose up (postgres + kafka + connect + engine + simulator)"
docker compose up -d --build

start=$(date +%s)

# 엔진 health UP 대기
until curl -sf "$REST/actuator/health" | grep -q '"status":"UP"'; do
  if (( $(date +%s) - start > TIMEOUT )); then log "TIMEOUT: 엔진 미기동"; exit 1; fi
  sleep 3
done
log "엔진 health UP"

# 판매자 대시보드 뷰가 실데이터를 반환할 때까지 대기 (아무 seller_id 하나)
while true; do
  if (( $(date +%s) - start > TIMEOUT )); then log "TIMEOUT: 뷰 데이터 없음"; exit 1; fi
  for seller in 1 2 3 4 5; do
    resp=$(curl -sf "$REST/views/seller-dashboard/$seller" || true)
    if [[ -n "$resp" ]] && echo "$resp" | jq -e '.values.order_count >= 0' >/dev/null 2>&1; then
      cnt=$(echo "$resp" | jq -r '.values.order_count')
      if [[ "$cnt" != "null" ]] && (( $(echo "$cnt" | cut -d. -f1) > 0 )); then
        log "PASS: seller-dashboard/$seller = $resp"
        exit 0
      fi
    fi
  done
  sleep 3
done
