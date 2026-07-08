# CDC 실시간 구체화 뷰 엔진

> 본 포트폴리오 1번. 기반 인프라. 로드맵 순서 첫 번째.

## 컨셉

DB 변경(CDC, Debezium) → Kafka Streams로 조인/집계 → 항상 최신인 구체화 뷰(materialized view)를 API로 제공 + 캐시 자동 무효화.

예시 시나리오: 주문 + 재고 + 정산 테이블 3개가 각자 바뀌는데, "판매자 대시보드 집계"는 항상 1초 내 최신.

## 왜 이거

- 희소성: GitHub 유사 레포 **1개** (2026-07-07 검증, `cdc materialized view real-time` 검색)
- 한국 JD: CDC는 토스/우아한형제들 기술블로그 단골 주제. Kafka Streams + exactly-once = 면접 화두 그 자체
- 스프링 순도 높음: 도메인 로직 전부 서버에 있음
- 킵한 2번(이상거래 엔진)과 기술 스택이 겹쳐 학습 시너지, 도메인은 완전 다름

## 핵심 난제 (= README 서사)

- 순서 뒤집힌 이벤트 처리
- 중복 전달 (exactly-once 보장)
- 스키마 변경 중 뷰 재구축
- 뷰와 원본 DB 정합성 검증

## 스택

Spring Boot, Kafka Streams, Debezium, PostgreSQL

## 2번 프로젝트와의 관계

분리 전략 채택: 이 엔진을 독립 범용 인프라로 만들고, 티켓팅 이상거래 엔진이 이걸 피처 스토어로 가져다 쓰는 첫 고객. "내가 만든 인프라를 내가 소비" 스토리. 레포 2개 확보.

이상거래 엔진이 필요로 하는 집계 = 전부 슬라이딩 윈도우 스트림 집계:
- 이 계정, 최근 10분 결제 시도 횟수
- 이 IP, 최근 1시간 계정 수
- 이 카드, 오늘 누적 결제액

## 차별화 공식 (S급 레포 헌팅에서 배운 것)

1. 장애 서사: 기능 나열 대신 "Kafka 끊기면? 스키마 바뀌는 중에 이벤트 오면?" 시나리오 중심 README
2. 정직한 비교: Kafka Streams 기본 기능/Materialize/RisingWave 대비 뭐가 다른지 명시
3. 재현 가능한 데모
4. 검증 수치화: 정합성 검증 테스트 규모, 처리량 벤치

## 데모 (명령어 3개)

```bash
# 1) 전체 스택 기동 (postgres + kafka + connect + engine + simulator)
docker compose up -d --build

# 2) 판매자 대시보드 뷰 조회 (시뮬레이터가 채운 실데이터)
curl -s http://localhost:8080/views/seller-dashboard/1 | jq

# 3) 정리
docker compose down -v
```

시뮬레이터 속도 조절: `SIM_RATE=500 docker compose up -d --build`.
데모 자동 검증: `bash scripts/demo-check.sh` (5분 타임아웃).

로컬 빌드/테스트 (Java 21 필요):

```bash
./gradlew :engine:test
```

## 검증 스크립트

```bash
# exactly-once 카오스 재시작 검증 (compose 필요): 처리 도중 kill -9 후 재시작해도 뷰 값이 기대값과 동일
bash scripts/chaos-restart-test.sh

# 정합성 검증: 소스 스냅샷 기대값 vs 서빙 뷰 비교
curl -X POST http://localhost:8080/admin/verify/seller-dashboard | jq

# 벤치마크: SIM_RATE events/sec, 지연 분포(p50/p95/p99) 산출 → bench/reports/ 에 리포트 생성
bash bench/run-bench.sh                          # 기본 SIM_RATE=10000, DURATION_SEC=600
SIM_RATE=1000 DURATION_SEC=300 bash bench/run-bench.sh
```

## 벤치 실측 (V3 → T1 지연 → T2 처리량, 2026-07-08)

측정 환경: 단일 호스트 docker (macOS, 8 CPU / 16GB, Docker 8 NCPU / ~7.75GB). postgres·kafka·connect·engine·simulator 전부 동일 호스트 공유. 지연 = Debezium 커밋 ts_ms 대비 엔진 뷰 반영 시각(`view.apply.latency` percentile histogram, 워밍업 제외 윈도우 델타). Kafka Streams EOS v2, RocksDB 기본.

**목표 (10,000 events/sec, 뷰 갱신 p95 ≤ 1s) 달성.** 두 단계 튜닝으로:
- **T1 (지연 바닥)**: V3에서 p50가 rate 무관 716ms로 고정된 원인은 Debezium `poll.interval.ms`(기본 500ms) 배치 누적 대기. `poll.interval.ms` 500→10, `max.batch.size` 2048→256으로 지연 바닥 제거.
- **T2 (처리량)**: 단일 스레드가 10,000/s를 못 따라가 lag 폭증(831k). 소스 토픽 파티션 1→8 + `num.stream.threads` 1→8로 병렬화 (조인이 아닌 groupByKey 집계라 repartition이 co-partitioning 자동 처리). 기본값 반영.

| SIM_RATE | 설정 | p50 | p95 | p99 | lag | 판정 |
|---|---|---|---|---|---|---|
| 1,000/s | V3 baseline (poll=500, 1파티션/1스레드) | 716ms | 1074ms | 1432ms | bounded | 미달 |
| 1,000/s | T1 (poll=10, batch=256) | 112ms | 179ms | 358ms | bounded | PASS |
| 10,000/s | T1만 (1파티션/1스레드) | >30s | >30s | >30s | 831k 단조증가 | FAIL (처리량) |
| 10,000/s | T1+T2 (8파티션/8스레드) | 134ms | **537ms** | 1432ms | bounded 0~12k | **PASS** |

최종 검증: 10,000 events/sec, **10분 지속**, 워밍업 60초 제외 → p50=134ms, **p95=537ms**, p99=1432ms, lag bounded, DLQ 0건 (bench/reports/20260708-122749.md). 회귀 없음: 정합성 quiesce 후 mismatch 0/100 verdict=pass, EOS exactly_once_v2 멀티파티션 무결(kill -9 재시작 후 count=200/sum=20100 정확).

재현: `bash bench/run-bench.sh` (기본 SIM_RATE=10000, 8파티션/8스레드, 10분). 상세·기여도 분리: `.planning/t1-latency-tuning-verification.md`, `.planning/t2-throughput-scaling-verification.md`, `bench/reports/`.

주의(정직 기록): 이 결과는 8코어 단일 호스트에서 파티션/스레드 8로 CPU를 거의 다 쓴 상태다. 더 높은 rate는 더 큰 호스트 또는 멀티 인스턴스(SPEC 범위 외)가 필요하다.

## 참고

- 레포: harshithrao07/distributed-scheduler (3★) - 장애복구 중심 서사의 모범 (Spring+Kafka+Redis 구성)
- 논문: NSDI'26 EROICA (차등 관측성) - 파이프라인 장애 진단 관점 참고
