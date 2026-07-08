# 프로젝트 상태 (STATE)

- 프로젝트: CDC 실시간 구체화 뷰 엔진 (포트폴리오 1번)
- 최종 상태: **v1 완성 (2026-07-08)**
- 최종 판정 근거: .planning/v4-completion-report.md

## 완성 요약

SPEC(.planning/SPEC.md) R1~R10 전 수용기준(AC1-1 ~ AC10-5) + 부정기준 N1~N4 충족.
- 구현: Plan 01~11 (커밋 329dccd ~ 78c688c)
- 런타임 검증: V1 → V1.5 → V2 → V3 → T1 → T2 → V4 (커밋 6be68f7 ~ 최신)
- 테스트: engine 59 tests / 17 classes, 0 failures
- 성능: 10,000 events/sec, 10분 지속, p95=537ms (목표 ≤1s 달성)
- 정합성: 10만+ 이벤트 mismatch 0/100, EOS kill -9 복구 정확

## 검증 사이클 로그

| 단계 | 산출 | 리포트 |
|---|---|---|
| V1 | 데모/헬스 재현 (AC10-1~3) + SIM_SEED 수정 | v1-demo-verification.md |
| V1.5 | E1 기동결함 수정 (AC10-4/5) | v1.5-e1-fix-verification.md |
| V2 | 정합성·카오스 (AC6-3/4, AC7-2) + rate/스크립트 수정 | v2-consistency-chaos-verification.md |
| V3 | 벤치 실측 + 하네스 수정 (AC9-1/3/4) | v3-benchmark-verification.md |
| T1 | 지연 바닥 튜닝 (poll.interval) | t1-latency-tuning-verification.md |
| T2 | 처리량 스케일링 (파티션/스레드) → AC9-2 달성 | t2-throughput-scaling-verification.md |
| V4 | 최종 SPEC 대조 + N4/AC8-1 스팟체크 + 완성 선언 | v4-completion-report.md |

## 최종 성능 config (기본값 반영됨)

- 소스 토픽 파티션 8, num.stream.threads 8 (T2)
- Debezium poll.interval.ms 10, max.batch.size 256 (T1)
- Kafka Streams EOS v2, commit.interval.ms 100, RocksDB 기본

## 재현 명령

```bash
# 데모 (명령어 3개)
docker compose up -d --build
curl -s http://localhost:8080/views/seller-dashboard/1 | jq
docker compose down -v

# 검증
bash scripts/demo-check.sh                 # 원커맨드 데모 (AC10-4)
bash scripts/chaos-restart-test.sh         # EOS 카오스 재시작 (AC6-3/4)
bash bench/run-bench.sh                     # 10,000/s p95≤1s 벤치 (AC9-2)
./gradlew :engine:test                      # 59 tests (로컬은 JDK 21 + IT는 DOCKER_API_VERSION=1.44)
```

## 잔여 (SPEC 명시적 범위 외 / 선택적 정비)

- 멀티 인스턴스 수평 확장 (10,000/s는 단일 8코어 호스트 상한)
- 윈도우 뷰 정합성 검증 (MVP 범위 밖, 엔진이 400 명시 거부)
- AC7-4/AC8-1 회귀 테스트 자동화 (V4 스팟체크로 실증됨, 자동화는 선택)
- testcontainers 버전 업 (로컬 IT는 DOCKER_API_VERSION=1.44 워크어라운드)

## 다음 로드맵

- 2번 프로젝트: 티켓팅 이상거래 엔진 (이 엔진을 피처 스토어로 소비)
