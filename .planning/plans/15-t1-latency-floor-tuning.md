# Plan 15 (T1): 지연 바닥 튜닝 실험

## 배경 / 가설

V3 (.planning/v3-benchmark-verification.md) 에서 지속가능 rate (1,000~2,000/s) 에서도 p50가 716ms 로 고정 (부하 무관) → p95≤1s 를 막는 건 처리량이 아니라 **파이프라인 고정 지연 바닥**. 지연 측정 지점은 Kafka Streams Aggregator 처리 시점 (TopologyBuilder.java:98, 커밋/캐시 flush 이전) 이므로 `commit.interval.ms`/`cache.max.bytes.buffering` 는 이 지표에 영향이 적다. 유력 원인:

- **Debezium `poll.interval.ms` (기본 500ms)**: 커넥터 task poll() 이 max.batch.size 만큼 이벤트가 쌓이거나 이 시간이 지날 때까지 블로킹. 1,000/s + max.batch.size 기본 2048 이면 배치가 안 차서 매 poll 이 500ms 대기 → 이벤트당 평균 수백 ms 지연.
- 보조: `max.batch.size`, Connect producer `linger.ms`, Kafka Streams `commit.interval.ms` (이 지표엔 영향 적을 것으로 예상하나 A/B로 확인).

## 목표 / 범위

- 지속가능 rate (1,000/s, 필요시 2,000/s) 에서 p95 ≤ 1,000ms 달성 가능한지 확인.
- **처리량 (T2, 10,000/s) 은 범위 밖.** 지연만 공략.
- config 값이 근거 있으면 실제 설정에 반영. 튜닝해도 미달이면 정직하게 하한 기록.

## 실험 설계 (한 번에 하나씩, 기여도 분리)

전부 SIM_RATE=1000, WARMUP=45, DURATION=90 기본 (유효 후보는 5분 재확인). 각 실험 p50/p95/p99 + lag + late/DLQ 기록.

| # | 변경 | 기대 |
|---|---|---|
| E0 | baseline (poll.interval.ms=500, max.batch.size=2048, commit.interval.ms=100) | p50~716 p95~1074 (V3 재현) |
| E1 | Debezium poll.interval.ms 500→10 | p50 대폭 하락 예상 |
| E2 | E1 + max.batch.size 2048→256 | 배치 대기 추가 감소 |
| E3 | (필요시) + Streams commit.interval.ms 100→50 | 미미 예상 (확인용) |

config 는 env 오버라이드로 주입 (재빌드 없이 A/B). 유효 조합은 application.yml 기본값에 반영.

## 부정 기준 (회귀 금지)

- 정합성: 각 유효 config 에서 V2식 스팟체크 (10만 미만이라도 quiesce 후 mismatch 0, verdict pass).
- EOS: processing.guarantee=exactly_once_v2 유지 (변경 안 함).
- late/DLQ: 전 실험 0건 유지.

## 검증

- [x] E1~E3 실측 p50/p95/p99 기록, 기여도 분리 - E1a(poll=10만): 1,000/s p50=134 p95~537. E1(poll=10+batch=256): 1,000/s p50=112 p95=179 p99=358. poll.interval.ms가 지배적, batch=256이 tail 개선. E3(commit.interval) 불필요 (E1이 목표 여유 달성). baseline(V3): p50=716 p95=1074
- [x] 최선 config 에서 지속가능 rate p95 ≤ 1,000ms 달성 여부 판정 - **달성**: 1,000/s p95=179ms, 2,000/s(5분) p95=537ms. 10,000/s는 T1 후에도 처리량 포화로 FAIL(lag 831k) → AC9-2는 처리량(T2)만 남긴 채 미달 유지, 지연 블로커는 해소
- [x] 유효 config 에서 정합성 mismatch 0 + late/DLQ 0 (회귀 없음) - E1 config에서 quiesce 후 verdict=pass, mismatchCount=0/100, late/DLQ 0, EOS 유지
- [x] README/벤치 리포트 갱신 - README 벤치 섹션 T1 결과 반영, bench/reports/ 4개 리포트, .planning/t1-latency-tuning-verification.md
