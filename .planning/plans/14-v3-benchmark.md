# Plan 14 (V3): 벤치 실측 + README 반영

## 배경

Plan 11에서 벤치 하네스 (bench/run-bench.sh, 엔진 `view.apply.latency` 히스토그램) 코드는 완료됐으나 step6 (실측 + README 반영) 미완. V1.5에서 O1 (시뮬레이터 rate 미달) 이 해소되어 이제 target rate 달성이 가능하므로 실측한다.

## 판정 기준 (SPEC R9)

- AC9-1: 하네스가 커밋 ts_ms 대비 뷰 반영 지연 분포 (p50/p95/p99) 를 산출해 리포트 파일 생성
- AC9-2: 10,000 events/sec, 10분 지속, 워밍업 60초 제외 조건에서 p95 ≤ 1,000ms (exit 0)
- AC9-3: 리포트에 하드웨어 사양 (CPU, 메모리, docker 리소스 한도) 기록
- AC9-4: 부하 중 처리 실패/DLQ 오적재 0건

## 측정 방법 / 파라미터

- 지연 기준점: Debezium source.ts_ms (PG 커밋 시각) 대비 엔진 state store 반영 시각 (`view.apply.latency` Timer, 뷰별 태그, Micrometer publishPercentiles 0.5/0.95/0.99). REST 폴링 아닌 엔진 내부 히스토그램 (정밀·저부하).
- 워밍업: 측정 시작 후 60초 데이터 제외. Micrometer 클라이언트 percentile 은 decaying window (기본 ~2-3분) 라 측정 종료 시점 p95 는 정상상태 (워밍업 훨씬 이후) 를 반영 - V2에서 기록한 deficit catch-up 버스트 우려는 워밍업 구간에 흡수됨.
- 파라미터: EOS v2, commit.interval.ms=100, num.stream.threads=1, RocksDB 기본. SIM_RATE=10000, DURATION_SEC=600, WARMUP_SEC=60, SCRAPE_INTERVAL=15.
- 하드웨어: 호스트 8 CPU / 16GB, Docker 8 NCPU / ~7.75GB (postgres/kafka/connect/engine/simulator 전부 동일 호스트 공유). 리포트에 자동 수집.

## 절차

1. 프로브 런 (SIM_RATE=10000, DURATION 짧게) 으로 10,000/s 지속 가능성 + lag 안정성 확인.
2. 지속 가능하면 정식 10분 런 → AC9-2 판정.
3. 미달 시 원인 구분:
   - 호스트 리소스 한계 (CPU 포화, 시뮬레이터/Debezium/Streams 경합): 달성 가능한 최대 rate 로 재측정하고 "이 하드웨어에서 X events/sec, p95 Y ms" 정직하게 기록. **목표 수치 위조 금지.**
   - 엔진 병목 (특정 단계 구조적 한계): 목록화 (수정은 별도 plan).
4. README 벤치 섹션에 실측 수치 + 사양 + 파라미터 + 재현 명령어 반영 (현재 목표치만 서술됨).

## 검증

- [x] AC9-1: 리포트 파일 생성 + p50/p95/p99 산출 (하네스 결함 2건 수정 후)
- [ ] AC9-2: 미달 (정직 기록) - 10,000/s p95 >30s (처리량 포화). 처리량 지속가능 ~2,000/s, p95≤1s는 rate 무관 미달 (p50 716ms 고정 지연 바닥). 사유: 호스트 리소스 한계 + 파이프라인 config 지연 바닥 (엔진 알고리즘 병목 아님)
- [x] AC9-3: 하드웨어 사양 리포트 기록 (8 CPU/16GB, Docker 8 NCPU/~7.75GB)
- [x] AC9-4: 처리 실패/DLQ 0건 (전 구간)
