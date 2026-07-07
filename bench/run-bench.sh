#!/usr/bin/env bash
# 벤치마크 하네스 (AC9-1 ~ AC9-4)
#
# 클린 compose 기동 → SIM_RATE=10000, 10분 실행 → 워밍업 60초 제외 구간의 지연 분포(p50/p95/p99)를
# 엔진 히스토그램(view.apply.latency, 기준점 = PG 커밋 시각)에서 스크레이프해 리포트를 생성한다.
# 판정: p95 <= 1000ms AND DLQ(지각) 오적재 0 → exit 0, 아니면 exit 1.
#
# 요구: docker, docker compose, curl, awk. (macOS/Linux)
set -euo pipefail

cd "$(dirname "$0")/.."

SIM_RATE="${SIM_RATE:-10000}"
DURATION_SEC="${DURATION_SEC:-600}"     # 측정 10분
WARMUP_SEC="${WARMUP_SEC:-60}"          # 워밍업 제외
SCRAPE_INTERVAL="${SCRAPE_INTERVAL:-15}"
VIEW="${VIEW:-seller-dashboard}"
REST="http://localhost:8080"
P95_TARGET_MS=1000

mkdir -p bench/reports
ts=$(date +%Y%m%d-%H%M%S)
report="bench/reports/${ts}.md"

log() { echo "[bench] $*"; }

prom() { curl -sf "$REST/actuator/prometheus" 2>/dev/null || true; }

# view.apply.latency 특정 quantile (seconds) → ms
quantile_ms() {  # quantile_ms <q>
  prom | awk -v q="$1" -v view="$VIEW" '
    $0 ~ /^view_apply_latency_seconds\{/ && $0 ~ ("quantile=\"" q "\"") && $0 ~ ("view=\"" view "\"") {
      print $NF * 1000; exit
    }'
}

counter_sum() {  # counter_sum <metric_prefix>
  prom | awk -v m="$1" '$1 ~ ("^" m) {s+=$NF} END {printf "%.0f", s+0}'
}

log "1) 클린 기동 (SIM_RATE=$SIM_RATE)"
docker compose down -v >/dev/null 2>&1 || true
SIM_RATE="$SIM_RATE" SIM_DURATION=$((DURATION_SEC + WARMUP_SEC + 120)) docker compose up -d --build

log "엔진 health 대기"
until prom | grep -q 'application_ready_time_seconds\|jvm_info' ; do sleep 3; done
until curl -sf "$REST/actuator/health" | grep -q '"status":"UP"'; do sleep 3; done

log "2) 워밍업 ${WARMUP_SEC}s (측정 제외)"
sleep "$WARMUP_SEC"

log "3) 측정 ${DURATION_SEC}s (${SCRAPE_INTERVAL}s 간격 스크레이프)"
received_start=$(counter_sum "cdc_events_received_total")
elapsed=0
p50=""; p95=""; p99=""
while (( elapsed < DURATION_SEC )); do
  sleep "$SCRAPE_INTERVAL"
  elapsed=$((elapsed + SCRAPE_INTERVAL))
  p50=$(quantile_ms 0.5); p95=$(quantile_ms 0.95); p99=$(quantile_ms 0.99)
  log "  t=${elapsed}s p50=${p50:-?}ms p95=${p95:-?}ms p99=${p99:-?}ms"
done
received_end=$(counter_sum "cdc_events_received_total")
late_events=$(counter_sum "view_late_events_total")

throughput=$(awk -v a="$received_start" -v b="$received_end" -v d="$DURATION_SEC" \
             'BEGIN{printf "%.0f", (b-a)/d}')

# 하드웨어 사양
cpu_cores=$( (sysctl -n hw.ncpu 2>/dev/null) || nproc 2>/dev/null || echo "?")
mem_bytes=$( (sysctl -n hw.memsize 2>/dev/null) || (awk '/MemTotal/{print $2*1024}' /proc/meminfo 2>/dev/null) || echo "?")
docker_ncpu=$(docker info --format '{{.NCPU}}' 2>/dev/null || echo "?")
docker_mem=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo "?")

# 판정
verdict="FAIL"
p95_int=$(awk -v p="${p95:-999999}" 'BEGIN{printf "%.0f", p}')
if awk -v p="${p95:-999999}" -v t="$P95_TARGET_MS" 'BEGIN{exit !(p<=t)}' && [[ "${late_events:-1}" == "0" ]]; then
  verdict="PASS"
fi

log "4) 리포트 생성: $report"
cat > "$report" <<EOF
# 벤치마크 리포트 ${ts}

## 판정: ${verdict}

- 지연 목표: p95 <= ${P95_TARGET_MS}ms
- DLQ(지각) 오적재: ${late_events} (목표 0, 의도 주입 없음)

## 지연 분포 (PG 커밋 ts_ms 대비 뷰 반영, view=${VIEW})

| 지표 | 값 |
|---|---|
| p50 | ${p50:-?} ms |
| p95 | ${p95:-?} ms |
| p99 | ${p99:-?} ms |

## 처리량

- 목표 SIM_RATE: ${SIM_RATE} events/sec
- 엔진 수신 처리량(측정 구간): ${throughput} events/sec
- 누적 수신: $((received_end - received_start)) events / ${DURATION_SEC}s

## 실행 조건

- 측정 시간: ${DURATION_SEC}s, 워밍업 제외: ${WARMUP_SEC}s
- 스크레이프 간격: ${SCRAPE_INTERVAL}s
- 실행 일시: $(date -u +"%Y-%m-%dT%H:%M:%SZ")

## 하드웨어 / 리소스

- 호스트 CPU 코어: ${cpu_cores}
- 호스트 메모리: ${mem_bytes} bytes
- Docker NCPU: ${docker_ncpu}
- Docker MemTotal: ${docker_mem} bytes
- 각주: PG와 엔진은 같은 docker 호스트 시계를 공유하므로 clock skew는 무시 가능(각 컨테이너 간 시계 동일).

## 튜닝 파라미터 (재현성)

- Kafka Streams: processing.guarantee=exactly_once_v2, commit.interval.ms=100, num.stream.threads=1
- auto.offset.reset=earliest, replication.factor=1
- Debezium: decimal.handling.mode=double, tombstones.on.delete=false
- RocksDB: 기본값

## 재현 명령어

\`\`\`bash
SIM_RATE=${SIM_RATE} DURATION_SEC=${DURATION_SEC} bash bench/run-bench.sh
\`\`\`
EOF

cat "$report"

if [[ "$verdict" == "PASS" ]]; then
  log "PASS"
  exit 0
else
  log "FAIL (p95=${p95:-?}ms, late=${late_events})"
  exit 1
fi
