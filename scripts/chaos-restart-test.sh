#!/usr/bin/env bash
# 카오스 재시작 검증 (AC6-3, AC6-4)
#
# 결정적 시드 데이터를 PG에 커밋하고, 처리 도중 엔진을 kill -9(SIGKILL) 한 뒤 재기동한다.
# 재시작 후 최종 뷰 값이 무중단 실행 시의 기대값과 동일하면 통과(exit 0), 아니면 실패(exit 1).
# EOS v2 + LSN 멱등 필터 덕분에 동일 커밋이 2회 반영되지 않아야 한다.
#
# 요구: docker, docker compose, curl, jq. 반복 실행 가능(매 실행 데이터 초기화 포함).
set -euo pipefail

cd "$(dirname "$0")/.."

SELLER_ID=1
N="${N:-200}"                       # 주문 건수
REST="http://localhost:8080"
GROUP="cdc-view-engine"             # Streams application.id = consumer group

# 기대값: amount = 1..N
EXPECTED_COUNT="$N"
EXPECTED_SUM=$(( N * (N + 1) / 2 ))

log() { echo "[chaos] $*"; }

wait_for() {  # wait_for <desc> <timeout_sec> <cmd...>
  local desc="$1" timeout="$2"; shift 2
  local start; start=$(date +%s)
  until "$@" >/dev/null 2>&1; do
    if (( $(date +%s) - start > timeout )); then
      log "TIMEOUT: $desc"; return 1
    fi
    sleep 2
  done
  log "OK: $desc"
}

psql_exec() {
  docker compose exec -T postgres psql -U app -d demo -v ON_ERROR_STOP=1 "$@"
}

engine_healthy() {
  curl -sf "$REST/actuator/health" | grep -q '"status":"UP"'
}

consumer_lag_zero() {
  local lag
  lag=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 --describe --group "$GROUP" 2>/dev/null \
        | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')
  [[ "${lag:-1}" == "0" ]]
}

log "1) 클린 기동"
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --build

wait_for "엔진 health UP" 180 engine_healthy
# 커넥터 등록 + 초기 스냅샷 안정화 대기
wait_for "커넥터 등록" 60 bash -c "curl -sf $REST/actuator/health >/dev/null && curl -sf http://localhost:8083/connectors | grep -q cdcview"
sleep 5

log "2) 결정적 시드 데이터 $N건 커밋 (seller_id=$SELLER_ID, amount=1..$N)"
psql_exec -c "DELETE FROM orders WHERE seller_id=$SELLER_ID;"
psql_exec -c "INSERT INTO orders(id, seller_id, product_id, amount, status)
              SELECT g, $SELLER_ID, 1, g, 'CREATED' FROM generate_series(1,$N) g
              ON CONFLICT (id) DO UPDATE SET amount=EXCLUDED.amount;"

log "3) 처리 도중 엔진 강제 종료(kill -9)"
sleep 2
docker compose kill -s KILL engine

log "4) 엔진 재기동"
docker compose up -d engine
wait_for "엔진 재기동 health UP" 180 engine_healthy

log "5) consumer lag 0 대기"
wait_for "lag 0" 120 consumer_lag_zero
sleep 3

log "6) 뷰 값 검증"
resp=$(curl -sf "$REST/views/seller-dashboard/$SELLER_ID")
echo "$resp"
actual_count=$(echo "$resp" | jq -r '.values.order_count')
actual_sum=$(echo "$resp" | jq -r '.values.total_order_amount')

log "기대: count=$EXPECTED_COUNT sum=$EXPECTED_SUM / 실제: count=$actual_count sum=$actual_sum"

# 부동소수 비교(정수 기대값이므로 반올림 비교)
if [[ "$actual_count" == "$EXPECTED_COUNT" ]] && \
   awk -v a="$actual_sum" -v e="$EXPECTED_SUM" 'BEGIN{exit !(a==e)}'; then
  log "PASS: 재시작 후 뷰 값이 기대값과 동일"
  exit 0
else
  log "FAIL: 불일치 (이중 반영 또는 유실 의심)"
  exit 1
fi
