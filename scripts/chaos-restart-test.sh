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
REST="${REST:-http://localhost:8080}"      # 호스트 8080 충돌 시 오버라이드
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
  # 재시작 직후 컨슈머 그룹이 아직 재등록 전이면 describe 출력이 비어 awk 합계가 0이 되어
  # 거짓 양성(lag 0)이 난다 → 데이터 행(LAG 숫자)이 최소 1건 존재하고 그 합이 0일 때만 참.
  local out rows lag
  out=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 --describe --group "$GROUP" 2>/dev/null)
  rows=$(echo "$out" | awk 'NR>1 && $6 ~ /^[0-9]+$/ {n++} END {print n+0}')
  lag=$(echo "$out" | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')
  [[ "${rows:-0}" -ge 1 && "${lag:-1}" == "0" ]]
}

# store가 쿼리 가능(HTTP 200)해지고 집계가 반영될 때까지 재시도한다.
# 재시작 직후 store 복원 중에는 일시적으로 500이 날 수 있다(엔진 정상 동작, 검증 타이밍 방어).
view_queryable() {
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' "$REST/views/seller-dashboard/$SELLER_ID")
  [[ "$code" == "200" ]]
}

log "1) 클린 기동 (시뮬레이터 제외 - 결정적 시드 보호, Plan 13)"
# 시뮬레이터를 함께 띄우면 엔진 기동 대기 동안 orders에 임의 id/seller를 INSERT/DELETE 해
# 결정적 시드 id 공간과 seller 배정을 오염시킨다. 카오스 검증은 시뮬레이터 없이 기동한다.
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --build postgres kafka connect engine

wait_for "엔진 health UP" 180 engine_healthy
# 커넥터 등록 + 초기 스냅샷 안정화 대기
wait_for "커넥터 등록" 60 bash -c "curl -sf $REST/actuator/health >/dev/null && curl -sf http://localhost:8083/connectors | grep -q cdcview"
sleep 5

log "2) 결정적 시드 데이터 ${N}건 커밋 (seller_id=$SELLER_ID, amount=1..$N)"
# id 1..N 전체를 비우고(다른 seller 소유 잔재 방지) 재삽입. ON CONFLICT는 seller_id까지 복원.
psql_exec -c "DELETE FROM orders WHERE seller_id=$SELLER_ID OR id BETWEEN 1 AND $N;"
psql_exec -c "INSERT INTO orders(id, seller_id, product_id, amount, status)
              SELECT g, $SELLER_ID, 1, g, 'CREATED' FROM generate_series(1,$N) g
              ON CONFLICT (id) DO UPDATE
                SET amount=EXCLUDED.amount, seller_id=EXCLUDED.seller_id, status=EXCLUDED.status;"

log "3) 처리 도중 엔진 강제 종료(kill -9)"
sleep 2
docker compose kill -s KILL engine

log "4) 엔진 재기동"
docker compose up -d engine
wait_for "엔진 재기동 health UP" 180 engine_healthy

log "5) consumer lag 0 대기"
wait_for "lag 0" 120 consumer_lag_zero
wait_for "뷰 쿼리 가능(store 복원 완료)" 60 view_queryable
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
