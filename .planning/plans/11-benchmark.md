# Plan 11: 벤치마크 하네스 + 지연/처리량 목표 검증

## 목표

DB 커밋 → 뷰 반영 지연 분포를 측정하는 벤치 하네스를 만들고, 10,000 events/sec에서 p95 ≤ 1초를 검증한다. SPEC R9 (AC9-1 ~ AC9-4) 를 구현한다.

## 컨텍스트

- 저장소: /Users/yunho/project/cdc-view-engine, SPEC: .planning/SPEC.md
- Plan 10: 시뮬레이터 (SIM_RATE로 속도 조절, 달성 rate 로그). Plan 05: freshness 메타 (응답에 sourceCommitTs, appliedAt). Plan 06: DLQ 토픽 + late/stale 카운터
- 지연 정의 (SPEC 확정): Debezium source.ts_ms (PG 커밋 시각) 와 뷰 반영 시각 (appliedAt) 의 차이. 측정 지점 - 엔진 내부에서 이벤트가 state store에 반영되는 순간 히스토그램 기록 (Micrometer `view.apply.latency`, percentile histogram). REST 폴링 방식보다 정밀하고 부하 적음. 보조로 REST end-to-end 샘플링 병행 가능
- 시계 주의: PG 컨테이너와 엔진 컨테이너가 같은 호스트 시계 공유 (docker) - clock skew 각주를 리포트에 포함
- 워밍업: 측정 시작 후 60초 데이터 제외 (SPEC 확정)
- 코드 위치 제안: bench/run-bench.sh + bench/report.py (또는 Java CLI - 구현 시 선택), 엔진 쪽은 Micrometer 히스토그램 추가만

## 작업

1. 엔진에 `view.apply.latency` 히스토그램 추가 (커밋 ts_ms 대비 반영 시각, 뷰별 태그, p50/p95/p99 노출)
2. bench/run-bench.sh: 클린 compose 기동 → SIM_RATE=10000, 10분 실행 → 워밍업 60초 이후 구간의 메트릭 스크레이프 (15초 간격) → 종료 후 리포트 생성
3. 리포트 생성: p50/p95/p99, 달성 처리량 (시뮬레이터 달성 rate + 엔진 수신 카운터), 처리 실패/DLQ 카운트, 하드웨어 사양 (sysctl/docker info로 CPU 코어, 메모리, docker 리소스 한도 수집), 실행 일시. bench/reports/{timestamp}.md 로 저장
4. pass/fail 판정 로직: p95 ≤ 1000ms AND DLQ 오적재 0 (의도 주입 제외 - 벤치 중에는 순서 역전 주입 없음이 기본) → exit 0, 아니면 exit 1
5. 튜닝 여지 기록: commit.interval.ms, RocksDB, Debezium poll 관련 파라미터를 리포트에 함께 기록 (재현성)
6. 결과를 README 벤치 섹션에 수치 + 사양 + 재현 명령어로 반영

## must-haves

- 지연 기준점은 반드시 PG 커밋 시각 (Kafka 도착 시각 아님)
- 워밍업 60초 제외가 리포트 산출 로직에 실제로 반영
- 리포트만 보고 제3자가 같은 벤치를 재현할 수 있는 정보 (사양, 파라미터, 명령어) 포함
- 목표 미달 시 스크립트가 fail로 종료 (수치 나열만 하고 통과 처리 금지)

## 검증

- [x] AC9-1: 벤치 하네스가 커밋 ts_ms 대비 뷰 반영 시각으로 지연 분포 (p50/p95/p99) 를 산출해 리포트 파일을 생성한다 - 2026-07-08 V3 실측: bench/reports/*.md 생성, p50/p95/p99 산출 (워밍업 제외 히스토그램 델타). 단 하네스 결함 2건 수정 (percentile 파싱, 워밍업 미제외) - .planning/v3-benchmark-verification.md
- [ ] AC9-2: 10,000 events/sec, 10분 지속, 워밍업 60초 제외 조건에서 p95 ≤ 1,000ms (run-bench.sh exit 0) - **미달 (10,000/s 처리량 한계, 정직 기록)**. V3(2026-07-08): 지연 바닥 p50 716ms로 rate 무관 미달. **T1(Plan 15)에서 지연 바닥 해소** - Debezium poll.interval.ms 500→10, max.batch.size 2048→256으로 지속가능 rate에서 p95 179ms(1,000/s)~537ms(2,000/s) 달성(≤1s). 그러나 10,000/s는 T1 후에도 p95 >30s, lag 831k 단조증가 → 사유가 지연에서 **처리량 포화만 남음**. 남은 과제 T2(스레드/파티션↑ 또는 멀티노드, SPEC 범위 외). 상세: .planning/t1-latency-tuning-verification.md
- [x] AC9-3: 리포트에 실행 하드웨어 사양 (CPU, 메모리, docker 리소스 한도) 이 기록된다 - 2026-07-08 실측: 호스트 8 CPU/16GB, Docker 8 NCPU/~7.75GB 리포트 자동 기록
- [x] AC9-4: 부하 중 처리 실패와 DLQ 오적재 0건 (리포트의 failure/DLQ 카운트로 확인) - 2026-07-08 실측: 전 rate 구간 late/DLQ 0건 (의도 주입 없음)

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
bash bench/run-bench.sh
cat bench/reports/*.md
```
