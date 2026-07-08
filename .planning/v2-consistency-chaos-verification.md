# V2 검증 리포트: 정합성·카오스 런타임 검증 (Plan 13)

- 일시: 2026-07-07 22:06 ~ 22:32 (KST)
- 환경: macOS (Darwin 25.5.0), Docker Desktop 29.4.3, compose 클린 스택. 엔진 호스트 8080 (충돌 없음)
- 범위: AC6-3 / AC6-4 (kill -9 카오스 재시작) + AC7-2 (10만 이벤트 정합성). V2는 검증 plan - 엔진 코드 결함은 수정하지 않고 목록화, 스크립트/시뮬레이터 결함은 수정
- 명세: .planning/plans/13-v2-consistency-chaos.md

## 사전 조사: O1 (시뮬레이터 achieved 75~78/100) - 시뮬레이터 결함, 수정

- 원인: `Simulator.run()` 이 10ms 창마다 상대 sleep (`windowMs - elapsed`) 하는데, DB 왕복 + `Thread.sleep` 오버슬립이 매 창 누적되어 실주기가 ~13ms 로 늘어남 (10/13 ≈ 0.77, 실측 75~78% 와 일치). 드리프트 보정 없음.
- 수정: deficit 방식으로 교체 - 매 루프 `expected = (elapsed+windowMs) * rate / 1000` 과 `emitted` 차이만큼 배치 커밋. 오버슬립이 다음 루프에서 자동 보정. 일시 정지 후 폭주 방지로 배치 상한 (`maxBatch = rate`, 1초 분량).
- 실측 (수정 후, SIM_RATE=300): `target=300 achieved=300 events/sec` 지속. 일시 stall (achieved=40) 직후 상한 내 catch-up 버스트 (achieved=2632) 가 한 번 관측됨 - deficit 특성상 정상 (장기 평균은 target 수렴). 10만 이벤트 도달 약 6분.

## AC 실측 결과

| AC | 판정 | 실측 |
|---|---|---|
| AC6-3 | PASS | N=200 결정적 시드 (amount=1..200) 처리 도중 kill -9 → 재기동 → lag 0 + store 복원 대기 → 최종 뷰 **count=200, sum=20100.0** (기대값 정확히 일치, 유실/중복 0) |
| AC6-4 | PASS | chaos-restart-test.sh 2회 연속 실행 (pass B, C) 전부 exit 0. 매 실행 down -v 초기화로 반복 가능 실증 |
| AC7-2 | PASS | SIM_RATE=300 으로 **102,655 이벤트** 주입 → 시뮬레이터 정지 (22:30:22) → 90초 대기 (consumer group 5파티션 total_lag=0 확인) → `POST /admin/verify/seller-dashboard` → **quiesced=true, verdict=pass, mismatchCount=0, 100/100 groups** (durationMs 42~89) |

### 카오스 경로 커버리지 (AC6-3)

chaos-restart-test.sh 는 매 실행 `down -v` → `up --build` 로 시작하므로 **클린 기동 경로** (Plan 12 토픽 선생성 경유) 와 **kill -9 재시작 경로** (EOS v2 상태 복원 + LSN 멱등 필터) 를 한 실행에서 모두 지난다. 재시작 시 로그상 store 복원 (lsn-guard 200 records, view changelog 복원) 후 REBALANCING → RUNNING 확인.

### AC7-2 세부

- 윈도우 뷰 (seller-sales-window) 검증은 엔진이 HTTP 400 "윈도우 뷰 검증은 MVP 범위 밖입니다" 로 **명시 거부** (`ExpectedViewSqlGenerator.java:23` 의 `if (def.isWindowed()) throw`). Plan 08 설계 노트 ("윈도우는 quiesce 상태 전제") 대로의 의도적 경계이며 조용한 실패가 아니다. AC7-2 는 뷰를 특정하지 않으며 (SPEC/Plan 08 테스트 명령도 seller-dashboard 만 예시), 조인 뷰 검증 통과로 충족.
- 검증기가 "항상 0" 가짜가 아님은 Plan 08 음성 테스트 (고의 오염 → 검출) 로 이미 고정됨. 본 실측은 실데이터 10만+ 규모 무결성 확인.

## 발견 결함과 처리

### 시뮬레이터 (수정)

- O1: 상대 sleep 드리프트 → deficit 방식 교체 (위 참조).

### 스크립트 (수정 - 카오스/데모 글루)

- S1: chaos-restart-test.sh line 64 `$N건` - non-interactive bash (ko_KR.UTF-8 미적용 환경) 에서 `$N` 뒤 한글 바이트가 변수명에 붙어 `unbound variable` 크래시 → `${N}건` 으로 명시.
- S2: `consumer_lag_zero` 거짓 양성 - 재시작 직후 컨슈머 그룹 미등록 시 describe 출력이 비어 awk 합계 0 → lag 0 오판정. 데이터 행 (LAG 숫자) 최소 1건 존재 + 합 0 일 때만 참으로 강화. 추가로 `view_queryable` (HTTP 200) 대기를 넣어 store 복원 중 일시적 500 로 인한 검증 조기 실패 방지.
- S3: 시뮬레이터 시드 오염 - `up --build` 가 시뮬레이터까지 띄워 엔진 기동 대기 동안 orders 에 임의 id/seller 를 INSERT. 결정적 시드의 `ON CONFLICT(id) DO UPDATE SET amount` 이 seller_id 를 복원하지 않아 seller_id=1 이 199행만 보유 → 뷰 count=199 (뷰는 PG 를 정확히 반영, 엔진 무결). 카오스 스택을 **시뮬레이터 제외** (`up -d ... postgres kafka connect engine`) 로 기동하고, 시드 DELETE 범위를 id 1..N 로 확대 + ON CONFLICT 에 seller_id/status 복원 추가.
- O2: demo-check.sh REST 주소 8080 하드코딩 → `REST="${REST:-http://localhost:8080}"` 환경변수화. chaos-restart-test.sh 도 동일 처리.

### 엔진 코드 결함

- 없음. AC6-3 재시작 무결성 (count/sum 정확), AC7-2 정합성 (mismatch 0/100) 모두 엔진이 통과. S3 의 count=199 는 소스 (PG) 자체가 199행이었던 테스트 하네스 문제로, 뷰는 소스를 정확히 반영했다 (오히려 정합성 검증기의 정확성을 방증).

## 사후 그릴링 기록

| 공격 | 확인 결과 |
|---|---|
| chaos 결과 200/20100 이 우연 아닌가 (kill 타이밍) | kill 은 시드 커밋 2초 후 - 일부만 처리된 상태에서 죽여야 재시작 복원을 검증. 2회 (B, C) 반복 전부 정확 일치 → 우연 아님 |
| lag 0 인데 store 500 은 엔진 결함 아닌가 | 재시작 직후 store 복원 중 정상 상태. lag 0 오판정(S2)이 이른 조회를 유발한 스크립트 문제. view_queryable 대기 추가 후 해소, 최종값 정확 |
| 10만 검증 mismatch 0 이 검증기 무력화 아닌가 | Plan 08 음성 테스트가 고의 오염 검출을 이미 고정. durationMs 42~89 로 실제 스캔 수행 (100 groups expected vs actual 대조) |
| 윈도우 뷰 미검증이 AC7-2 미달 아닌가 | AC7-2 는 뷰 불특정. 윈도우는 엔진이 명시 거부하는 문서화된 MVP 경계 (조용한 폐기 아님). 조인 뷰로 충족 |
| deficit 버스트 (2632) 가 R9 부하 왜곡 아닌가 | maxBatch=rate 상한으로 1초 분량 제한. Plan 11 벤치는 별도 - 여기선 정합성이 목적이라 무영향 (quiesce 후 lag 0 확인) |

## 남은 항목 / 후속

- 윈도우 뷰 정합성 검증 (ExpectedViewSqlGenerator 윈도우 버킷 SQL) - 현재 MVP 범위 밖. 필요 시 별도 plan.
- testcontainers docker-java API 버전 문제 (V1.5에서 기록) - 버전 업 정비 항목. 본 V2는 compose 런타임 검증이라 무관.
- Plan 11 (벤치, 10,000 events/sec, p95≤1s) 는 다음 plan. O1 해소로 시뮬레이터가 이제 target rate 를 달성하므로 벤치 전제 충족.
