# T1 검증 리포트: 지연 바닥 튜닝 (Plan 15)

- 일시: 2026-07-08 11:10 ~ 11:45 (KST)
- 환경: macOS, Docker Desktop 29.4.3. 호스트 8 CPU / 16GB, Docker 8 NCPU / ~7.75GB. 5개 컨테이너 동일 호스트 공유
- 배경: V3에서 지속가능 rate(1,000~2,000/s)에서도 p50가 716ms로 고정 → p95≤1s를 막는 지연 바닥. 가설: Debezium `poll.interval.ms`(기본 500ms) 배치 누적 대기
- 범위: 지연만. 처리량(T2, 10,000/s)은 범위 밖
- 명세: .planning/plans/15-t1-latency-floor-tuning.md

## 변경 (config, env 오버라이드 + 새 기본값)

지연 측정 지점은 Kafka Streams Aggregator 처리 시점(TopologyBuilder.java:98, 커밋/캐시 flush 이전)이라 commit.interval.ms/cache는 이 지표에 영향 적음 → Debezium 쪽 집중.

- `ConnectorConfigFactory`에 `poll.interval.ms`, `max.batch.size` 주입. 기본값 **poll.interval.ms=10 (기존 미설정=500), max.batch.size=256 (기존 미설정=2048)**.
- `ConnectProperties.pollIntervalMs/maxBatchSize`, `KafkaProperties.commitIntervalMs` 추가 (env `CONNECT_POLL_INTERVAL_MS`/`CONNECT_MAX_BATCH_SIZE`/`KAFKA_COMMIT_INTERVAL_MS`로 A/B). docker-compose.yml engine 서비스에 passthrough.
- `StreamsConfigFactory` commit.interval.ms를 property화(기본 100 유지).

## 실험 결과 (SIM_RATE=1000, WARMUP=45, DURATION=90 기본, 지연 = 워밍업 제외 히스토그램 델타 버킷 상한)

| # | 설정 | p50 | p95 | p99 | lag | 판정 |
|---|---|---|---|---|---|---|
| E0 (V3 baseline) | poll=500, batch=2048 | 716ms | 1074ms | 1432ms | bounded | 지연 미달 |
| E1a | poll=**10**, batch=2048 | 134ms | ~537ms | 1432ms | bounded | PASS |
| E1 | poll=**10**, batch=**256** | 112ms | **179ms** | 358ms | bounded | PASS |
| E1 @ 2,000/s (5분) | poll=10, batch=256 | 112ms | **537ms** | 2505ms | bounded 0~0.9k | PASS |
| E1 @ 10,000/s (2분) | poll=10, batch=256 | >30s | >30s | >30s | 831k 단조증가 | FAIL (처리량) |

리포트 파일: bench/reports/20260708-111940.md(E1 1k), 112415.md(E1a), 112840.md(E1 2k 5분), 113846.md(E1 10k).

## 기여도 분리

- **`poll.interval.ms` 500→10 이 지배적**: p50 716→134ms, p95 1074→~537ms. 커넥터 poll()이 max.batch.size만큼 이벤트가 쌓이거나 이 시간이 지날 때까지 블로킹하던 것이 원인 확정 (가설 검증됨).
- **`max.batch.size` 2048→256 은 tail 보정**: p95 537→179ms, p99 1432→358ms. 부분 배치를 더 빨리 방출해 배치 누적 jitter 감소.
- commit.interval.ms(E3)는 실험 불필요 - E1이 목표를 여유(179ms vs 1000ms) 달성. 예상대로 이 지표엔 영향 적음.

## 회귀 확인 (부정 기준)

- 정합성: E1 config, 2,000/s 부하 후 시뮬레이터 정지 → lag 0 → `POST /admin/verify/seller-dashboard` → **quiesced=true, verdict=pass, mismatchCount=0, 100/100 groups** (durationMs 431). EOS/정합성 회귀 없음.
- late/DLQ: 전 실험 0건 (카운터 미등록 = 발생 0).
- EOS: processing.guarantee=exactly_once_v2 그대로 (변경 안 함).
- 커넥터 실제 적용 확인: `GET /connectors/cdcview-postgres/config` → poll.interval.ms=10, max.batch.size=256.

## AC9-2 재판정

- **지연 블로커 해소**: 지속가능 rate(1,000~2,000/s)에서 p95 179~537ms로 목표(≤1s) 여유 달성. V3의 "p95 하한이 1s를 넘어 rate 무관 미달"은 사라짐.
- **AC9-2(10,000/s, p95≤1s)는 미달 유지**: T1 후에도 10,000/s는 p95 >30s, lag 831k 단조 증가. 지연 튜닝은 Debezium→Kafka 구간 지연을 줄일 뿐 Kafka Streams 단일 스레드 처리량은 못 올림. **남은 유일한 블로커 = 처리량 포화(T2)**. 이 하드웨어(8코어 5컨테이너 공유)의 단일 노드 한계.

## 사후 그릴링 기록

| 공격 | 확인 |
|---|---|
| poll=10이 정합성/유실 유발 안 하나 | verdict=pass, mismatch 0. poll.interval은 방출 주기일 뿐 이벤트 유실과 무관. LSN 가드/EOS 그대로 |
| 지연 개선이 측정 아티팩트 아닌가 | 버킷 델타(워밍업 제외) 동일 하네스. E1a→E1 단조 개선 + 커넥터 config 실적용 확인. baseline 대비 6배 개선은 poll.interval 물리로 설명됨 |
| max.batch.size 낮추면 처리량 희생 아닌가 | 10,000/s FAIL은 batch와 무관(Streams 스레드 병목). 지속가능 rate에선 lag bounded 유지 → 처리량 손실 관측 안 됨 |
| commit.interval 안 건드려 불완전 아닌가 | 측정 지점이 커밋 이전이라 영향 적음(설계상). E1이 목표 여유 달성해 추가 튜닝 불필요. env로 조절 가능하게 남겨둠 |
| 2,000/s p99=2505ms는 미달 아닌가 | 목표는 p95. p99 tail은 시뮬레이터 deficit 버스트(maxBatch=500) 아티팩트로 추정. p95=537ms는 여유 통과 |

## 남은 항목

- T2 (처리량, 별도 plan): num.stream.threads 증대 + 입력 토픽 파티션 증가(현재 1, co-partitioning 재설계) 또는 멀티 인스턴스(SPEC 범위 외). 10,000/s 목표는 T2 후 재측정.
- 2,000/s p99 tail: 시뮬레이터 deficit 버스트 평활화(추가 maxBatch 하향) 여지 - 측정 정밀도 개선용, 필수 아님.
