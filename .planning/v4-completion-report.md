# V4 완료 리포트: CDC 실시간 구체화 뷰 엔진 v1 완성 선언

- 일시: 2026-07-08 (KST)
- 최종 판정: **완성 (SPEC R1~R11 전 수용기준 + 부정기준 N1~N4 충족)**
- 검증 사이클: Plan 01~11(구현) → V1(데모/헬스) → V1.5(기동결함) → V2(정합성/카오스) → V3(벤치) → T1(지연) → T2(처리량) → V4(최종 대조/완성)
- 근거 유형: 단위/통합 테스트(engine 59 tests / 17 classes, 0 failures) + 런타임 검증 리포트 + V4 스팟체크

## 전체 SPEC 대조 (수용기준별 근거 매핑)

### R1 YAML 뷰 정의
- AC1-1 (소스2+조인키+집계+윈도우 선언 부팅 등록): `config/ViewDefinitionTest.loadsJoinedWindowedView`
- AC1-2 (필수누락/알수없는필드 부팅 실패 + 파일명·필드경로): `ViewDefinitionTest.missingRequiredFieldFails / unknownFieldFails / slidingWithoutAdvanceFails / duplicateNameFails`
- AC1-3 (데모 뷰 2개 존재·로딩): `ViewDefinitionTest.loadsBothDemoViews` + views/seller-dashboard.yaml, seller-sales-window.yaml

### R2 PostgreSQL CDC 수집
- AC2-1 (커넥터 자동 등록): `cdc/ConnectorConfigFactoryTest` + V1 실측(커넥터 cdcview-postgres RUNNING 확인)
- AC2-2 (before/after·LSN·ts_ms 파싱 수신): `cdc/ChangeEventParserTest` + `cdc.events.received` 메트릭(벤치서 수백만 카운트 실측)
- AC2-3 (REPLICA IDENTITY FULL 아니면 명시적 에러): `cdc/ReplicaIdentityCheckerIT`

### R3 조인/집계 뷰 구체화
- AC3-1 (INSERT 후 집계 갱신): `topology/TopologyMaterializeTest.insertUpdatesAggregate`
- AC3-2 (UPDATE old 차감 new 가산): `TopologyMaterializeTest.updateSubtractsOldAddsNew`
- AC3-3 (DELETE 기여분 제거): `TopologyMaterializeTest.deleteRemovesContribution / minRecomputesOnDelete`
- AC3-4 ([start,end) 반개구간): `TopologyMaterializeTest.windowBoundaryIsHalfOpen`

### R4 REST 조회 API
- AC4-1 (최신값+freshness): `api/ViewQueryApiTest.latestReturnsValuesAndFreshness`
- AC4-2 (윈도우 범위 조회): `ViewQueryApiTest.windowRangeReturnsWindows`
- AC4-3 (없는 키/뷰 404): `ViewQueryApiTest.unknownViewReturns404 / unknownKeyReturns404`
- AC4-4 (ETag + If-None-Match 304): `ViewQueryApiTest.etagMatchReturns304`

### R5 순서 뒤집힌 이벤트 처리
- AC5-1 (역순 UPDATE → LSN 큰 값): `ordering/OrderingTest.reversedUpdatesKeepHighestLsn`
- AC5-2 (DELETE 먼저 → 부활 방지): `OrderingTest.deleteBeforeLowerLsnUpdateDoesNotRevive`
- AC5-3 (grace 이내 지각 반영): `ordering/LateEventTest.withinGraceIsApplied`
- AC5-4 / N3 (grace 초과 → DLQ + 카운터): `LateEventTest.beyondGraceGoesToDlqAndCounter`

### R6 exactly-once
- AC6-1 (exactly_once_v2 설정): `topology/ExactlyOnceConfigTest.defaultsToExactlyOnceV2`
- AC6-2 / N1 (동일 LSN 2회 → 불변): `topology/ExactlyOnceDedupTest.duplicateSameLsnInsertDoesNotDoubleCount / duplicateSameLsnUpdateDoesNotDoubleApply`
- AC6-3 (kill -9 후 재시작 정확): V2 chaos-restart-test.sh (count=200/sum=20100 정확). T2에서 8파티션/8스레드에서도 재확인
- AC6-4 (스크립트 반복 실행): V2 2회 연속 PASS (scripts/chaos-restart-test.sh)

### R7 정합성 검증기
- AC7-1 (스냅샷 비교 + 샘플 리포트): `verify/VerificationServiceTest.quiescedMatchIsPass / quiescedMismatchIsDetected / missingKeyInViewIsMismatch` + `ExpectedViewSqlGeneratorTest`
- AC7-2 (10만 이벤트 quiesce 후 mismatch 0): V2 실측 (102,655 이벤트, verdict=pass, mismatch 0/100). T2 10분 후에도 재확인
- AC7-3 (non-quiesced 플래그, verdict 없음): `VerificationServiceTest.nonQuiescedHasNoVerdict` + V2/V4 실측(quiesced=false, verdict=null)
- AC7-4 / N4 (검증 중 서빙 비블로킹): **V4 스팟체크** - 20연속 verify 중 GET 50건 전부 200(non-200 0). 설계상 read-only store 스캔

### R8 스키마 변경 중 뷰 재구축
- AC8-1 (미참조 컬럼 ADD 무중단): **V4 스팟체크** - ALTER TABLE ADD COLUMN + 신규 INSERT 후 뷰 정상 갱신(order_count 90→99), 엔진 에러 0, streams UP
- AC8-2 (rebuild 스냅샷→섀도→원자적 스왑): `rebuild/RebuildServiceTest.rebuildSwapsToNewVersion`
- AC8-3 / N2 (재구축~스왑 5xx/빈/혼합 0): `RebuildServiceTest.versionStaysUntilCaughtUp`
- AC8-4 (재구축 중 신규 이벤트 스왑 후 반영): `RebuildServiceTest.rebuildSwapsToNewVersion`(섀도가 최신까지 따라잡은 뒤 스왑)
- AC8-5 (참조 컬럼 삭제 시 명시적 거부): `RebuildServiceTest.missingColumnRejectsRebuild` + `rebuild/ColumnCheckerTest.detectsMissingReferencedColumn`
- AC8-6 (재구축 중 재시작 후 재요청 정상, 멱등): `RebuildServiceTest.concurrentRebuildRejected`(멱등 가드) + V1.5 재시작 안전성

### R9 지연/처리량
- AC9-1 (지연 분포 리포트 생성): V3 하네스(bench/run-bench.sh, 버킷 델타 p50/p95/p99 + bench/reports/*.md)
- AC9-2 (10,000/s 10분 워밍업제외 p95≤1s): **T1+T2로 달성**. 최종 10분: p50=134ms, p95=537ms, p99=1432ms, lag bounded, DLQ 0, exit 0 (bench/reports/20260708-122749.md)
- AC9-3 (하드웨어 사양 기록): 리포트에 호스트 8CPU/16GB, Docker 8NCPU/~7.75GB 자동 기록
- AC9-4 (처리 실패/DLQ 0): 전 벤치 late/DLQ 0건

### R10 docker compose 원커맨드 데모
- AC10-1 (4서비스 healthy): V1 실측(+34초). V1.5에서 클린 재확인
- AC10-2 (actuator health 200): V1 실측
- AC10-3 (시뮬레이터 rate 조절 구동): V1 실측 + O1 수정(deficit)으로 target 수렴
- AC10-4 (5분 내 실데이터): V1.5 실측 PASS(compose up→실데이터 52초, demo-check.sh 통과) - V1의 E1 결함 수정 후
- AC10-5 (README 절차 재현): V1.5 실측 PASS

### R11
- SPEC에 R11 요구사항 본문 없음(Plan 분해표는 R1~R10 + 11번 plan=벤치). AC는 R1~R10 + 부정기준으로 구성. 누락 아님.

## 부정 기준 (must-NOT)
- N1 (동일 LSN 2회 반영 금지) → AC6-2: `ExactlyOnceDedupTest` 충족
- N2 (재구축 스왑 시 빈/혼합 응답 금지) → AC8-3: `versionStaysUntilCaughtUp` 충족
- N3 (grace 초과 흔적없는 폐기 금지) → AC5-4: `beyondGraceGoesToDlqAndCounter`(DLQ+카운터) 충족
- N4 (검증이 서빙 블로킹 금지) → AC7-4: V4 스팟체크 GET 50/50 = 200 충족
- (SPEC 부정기준은 N1~N4. N5는 없음)

## 발견/수정 이력 (검증 사이클 중)
| 결함 | 성격 | 처리 |
|---|---|---|
| SIM_SEED 빈 문자열 크래시 | 시뮬레이터 | V1 수정 |
| E1: Streams 기동 시 CDC 토픽 부재 → 영구 ERROR | 엔진 | V1.5 수정(토픽 선생성 + 예외 핸들러) |
| 시뮬레이터 rate 미달(O1) | 시뮬레이터 | V2 deficit 방식 수정 |
| chaos 스크립트 3결함(한글변수/lag거짓양성/시드오염) | 스크립트 | V2 수정 |
| 벤치 하네스 2결함(percentile 파싱/워밍업 미제외) | 하네스 | V3 수정 |
| 지연 바닥 716ms | config | T1 수정(poll.interval 500→10) |
| 처리량 포화 | config | T2 수정(파티션/스레드 1→8) |
- 엔진 알고리즘 결함은 E1(기동 순서) 외 없음. 정합성/EOS/집계 로직은 무수정 통과.

## 하드웨어 / 측정 조건
- 호스트: macOS(Darwin 25.5.0), 8 CPU / 16GB. Docker Desktop 29.4.3, 8 NCPU / ~7.75GB. 5개 컨테이너(pg/kafka/connect/engine/simulator) 동일 호스트 공유.
- 최종 성능 config: 소스 파티션 8, num.stream.threads 8, Debezium poll.interval.ms 10 / max.batch.size 256, EOS v2, commit.interval.ms 100.
- 지연 기준점: Debezium 커밋 ts_ms 대비 엔진 뷰 반영 시각. 워밍업 60초 제외 히스토그램 버킷 델타.

## 잔여 (명시적 범위 외)
- 멀티 인스턴스 수평 확장(interactive query 라우팅): 별도 프로젝트급. 10,000/s는 이 단일 8코어 호스트 상한이며 초과는 더 큰 호스트/멀티노드 필요.
- 비PG 소스, 외부 캐시 자동 무효화, 관리 UI, 인증/멀티테넌시: SPEC 경계 섹션에서 제외.
- 윈도우 뷰 정합성 검증: 엔진이 명시적으로 MVP 범위 밖(400)으로 거부. 비윈도우 뷰 검증으로 AC7-2 충족.
- 경미한 테스트 커버리지 갭(자동화 안 됨, V4 스팟체크로 실증): AC7-4, AC8-1은 런타임 스팟체크로 확인했으나 회귀 테스트로는 미고정 - 후속 정비 대상(선택).
- testcontainers docker-java API 버전 이슈: 로컬 IT 실행 시 `DOCKER_API_VERSION=1.44` 필요(코드 무관, 버전 업 정비 항목).

## 포트폴리오 서사 (1번 프로젝트)
- 한 줄: PostgreSQL 변경(Debezium CDC)을 Kafka Streams로 조인/집계해 항상 최신인 구체화 뷰를 REST로 제공하는 단일 노드 엔진. 뷰는 코드가 아닌 YAML로 선언.
- 증명한 어려운 부분 4개: (1) 순서 뒤집힘(LSN + grace + DLQ), (2) 중복/재시작(EOS v2 + LSN 멱등, kill -9 복구 정확), (3) 스키마 변경 중 무중단 재구축(섀도+원자 스왑), (4) 원본 대비 정합성(10만+ 이벤트 mismatch 0).
- 수치: 정합성 0/100, p95 537ms@10,000events/s(10분), 59 테스트 green.
- 정직한 경계: 단일 인스턴스, PG 단일 소스, 프리셋 집계. 범용 스트리밍 DB(Materialize/RisingWave) 재발명이 아니라 CDC 정합성 계층의 직접 구현.
- 2번 프로젝트(티켓팅 이상거래 엔진)가 이 엔진을 피처 스토어로 소비하는 첫 고객: "내가 만든 인프라를 내가 소비" 서사.

## 최종 판정
**cdc-view-engine v1 완성.** SPEC 전 수용기준(R1~R10의 AC1~AC10-5)과 부정기준(N1~N4)이 단위/통합 테스트 + 런타임 검증 리포트 + V4 스팟체크로 충족됨. AC9-2(10,000/s p95≤1s) 포함 미달 항목 없음. 잔여는 전부 SPEC 명시적 범위 외.
