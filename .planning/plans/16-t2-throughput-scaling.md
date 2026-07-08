# Plan 16 (T2): 처리량 스케일링

## 배경 / 목표

V3+T1 후 AC9-2의 유일한 잔여 블로커는 처리량. 10,000/s에서 Kafka Streams 단일 스레드가 못 따라가 lag가 831k로 단조 증가 (지연은 T1으로 p95 179ms 확보). 목표: 지속가능 처리량을 ~2,000/s → 10,000/s로 끌어올려 p95≤1s + lag bounded + 정합성 0 동시 달성. 안 되면 이 하드웨어 상한을 정직 기록.

## 토폴로지 구조 (co-partitioning 안전성 분석)

TopologyBuilder는 **KStream-KStream 조인이 아니다**: 각 소스 stream → LsnGuard(FixedKey, 소스 PK 키 유지) → selectKey(groupBy=seller_id) → 전 소스 merge → groupByKey → aggregate. 즉 단일 그룹 스트림 집계.

- LsnGuard: FixedKey로 소스 토픽 PK 파티셔닝 유지 → 같은 PK 같은 파티션 → LSN 순서/중복 폐기 정합 (파티션 수 무관).
- selectKey(seller_id) → groupByKey: Streams가 repartition 토픽을 **소스 파티션 수와 동일하게 자동 생성**, seller_id 해시로 재분배 → 같은 seller 같은 태스크 → 집계 정합. **수동 co-partitioning 불필요** (조인이 없으므로).
- 결론: 소스 CDC 토픽 파티션 N 증설 + num.stream.threads N 증설이 정합성을 깨지 않는다 (자동 repartition). changelog/repartition 내부 토픽도 N으로 자동 생성. 매 실험 down -v 클린 기동이라 파티션 재생성 충돌 없음.

## 실험 설계 (기여도 분리, 한 번에 하나씩)

기본 SIM_RATE=10,000, WARMUP=45, DURATION=120 (유효 후보는 재확인). config는 env 오버라이드 (CONNECT_SOURCE_PARTITIONS, KAFKA_STREAM_THREADS).

| # | partitions | threads | 의도 |
|---|---|---|---|
| B0 | 1 | 1 | V3/T1 baseline (10k FAIL 재현) |
| S1 | 4 | 4 | 1차 스케일 (8코어 중 엔진 몫) |
| S2 | 8 | 8 | 코어 상한까지 |
| S3 | (최선 partitions) | 조정 | thread/partition 균형 미세조정 |

각 실험: p50/p95/p99, lag 추이(단조증가 여부), 달성 처리량, late/DLQ 기록. lag가 bounded면 지속가능.

파티션 수 근거: Docker 8 NCPU, 5개 컨테이너 공유. 엔진 Streams가 병렬 대상이나 kafka/pg/connect/simulator도 CPU 경쟁. Debezium PG capture는 단일 replication slot(단일 task)이라 capture 자체는 직렬 - 파티션은 produce 분배 + Streams 병렬에만 기여. 4/8로 A/B해 CPU 포화 지점 확인. 파티션은 재생성 비용이 크므로 실측 근거로 확정.

## 부정 기준 (회귀 금지, 최우선)

- **정합성**: 각 유효 config에서 V2식 전수 정합성 - 부하 후 시뮬레이터 정지 → lag 0 → POST /admin/verify → mismatchCount=0, verdict=pass. 파티션 증설이 조인/집계를 깨면 즉시 실패.
- EOS: exactly_once_v2가 멀티 스레드/멀티 파티션에서 유지 (설정 유지 + 정합성으로 간접 검증).
- late/DLQ: 부하 중 0건 (의도 주입 없음).
- 지연: T1 확보분 유지 - 지속가능 config에서 p95 ≤ 1s.

## 검증

- [x] B0/S1/S2(/S3) 실측 p50/p95/p99 + lag 추이 + 처리량, 기여도 분리 - B0(1/1): lag 831k 단조증가 FAIL. S1(4/4): lag 30~90k 느린감소, p95 22906ms FAIL. S2(8/8): lag 4~17k bounded, p95 895~1790ms 경계. S3(8/8+SIM_MAX_BATCH=150 평활화): p95=447ms PASS. 파티션/스레드 8이 처리량 임계, 부하 평활화가 p95 tail 마무리
- [x] 10,000/s에서 p95 ≤ 1s AND lag bounded 달성 여부 - **달성**. 최종 10분 지속: p50=134 p95=537 p99=1432ms, lag bounded(0~12k 배수), DLQ 0 → AC9-2 [x]
- [x] 각 유효 config 정합성 mismatch 0 + verdict pass + late/DLQ 0 (회귀 없음) - 8파티션에서 verdict=pass mismatch 0/100. EOS: chaos-restart 8/8에서 count=200 sum=20100 정확(멀티파티션 무결). late/DLQ 0
- [x] 근거 있으면 실제 기본값 반영, README/리포트 갱신, AC9-2 재판정 - 기본값 source-partitions=8, stream-threads=8 반영. README/리포트 갱신, AC9-2 달성 판정
