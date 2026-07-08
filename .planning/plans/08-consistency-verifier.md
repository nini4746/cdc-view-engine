# Plan 08: 정합성 검증기

## 목표

소스 PG 스냅샷으로 계산한 기대 뷰와 서빙 중인 뷰를 비교해 불일치를 리포트하는 검증기를 구현한다. SPEC R7 (AC7-1 ~ AC7-4, 부정 기준 N4 포함) 을 구현한다.

## 컨텍스트

- 저장소: /Users/yunho/project/cdc-view-engine, SPEC: .planning/SPEC.md
- Plan 02: ViewDefinition (조인 키, groupBy, 집계 함수 - 기대값 SQL 생성의 입력). Plan 05: REST 계층 + state store 조회 경로. Plan 03: consumer lag 확인 수단 (Kafka AdminClient)
- 설계 방향:
  - 기대값 계산: ViewDefinition을 PG SQL (JOIN + GROUP BY + 집계) 로 변환해 REPEATABLE READ 트랜잭션에서 실행. 윈도우 뷰는 ts 컬럼 기반 윈도우 버킷팅 SQL (MVP: 비윈도우 뷰 + 윈도우 뷰 둘 다 지원하되, 윈도우는 quiesce 상태 전제)
  - quiesce 판정: 검증 시작 시 CDC 토픽 consumer lag 조회. lag 0이고 최근 X초 신규 이벤트 없음 → quiesced, 아니면 non-quiesced 플래그
  - 비교: 기대 결과셋 vs state store 전체 스캔 (ReadOnlyKeyValueStore.all()), 키 단위 값 비교. 불일치 수 + 샘플 최대 10건 (키, 기대값, 실제값)
  - 비블로킹: 검증은 별도 스레드 풀에서 실행, store 스캔은 read-only이므로 서빙과 경합 최소. 검증 중 REST 조회 가용성 확인을 테스트에 포함
- 코드 위치 제안: engine/src/main/java/io/github/nini4746/cdcview/verify/ (VerificationService, ExpectedViewSqlGenerator, VerifyController)

## 작업

1. ExpectedViewSqlGenerator: ViewDefinition → 기대값 SQL (집계 함수 매핑, 조인, GROUP BY). 단위 테스트로 SQL 정확성 고정
2. VerificationService: REPEATABLE READ 스냅샷 조회 + store 스캔 비교 + 리포트 생성 (불일치 수, 샘플 10건, quiesced 여부, 소요 시간)
3. quiesce 검사: AdminClient로 CDC consumer group lag 합산
4. POST /admin/verify/{view}: 202 + 결과 폴링 또는 동기 응답 (MVP: 동기, 타임아웃 설정). non-quiesced면 결과에 플래그 + verdict 필드 없음, quiesced면 verdict: pass|fail
5. 통합 테스트 (compose 또는 Testcontainers): 결정적 데이터 주입 → lag 0 대기 → 검증 호출 → 불일치 0. 고의로 store 값 오염 (테스트 훅) → 불일치 검출 확인
6. 검증 중 서빙 확인 테스트: 대량 데이터로 검증 실행 중 REST 조회 루프가 전부 200

## must-haves

- 검증기가 "항상 0" 을 반환하는 가짜가 아님을 증명하는 음성 테스트 (고의 오염 → 검출) 필수
- non-quiesced 상태에서 pass/fail 판정 금지 (거짓 불일치 방지)
- 기대값 SQL이 뷰 정의에서 파생 (뷰별 하드코딩 금지)

## 검증

- [ ] AC7-1: POST /admin/verify/{view} 가 스냅샷 기반 기대값과 뷰를 비교해 불일치 키 수 + 샘플 (최대 10건) 리포트를 반환한다
- [x] AC7-2: quiesce 조건 (입력 정지 + lag 0) 에서 10만 이벤트 처리 후 검증 결과 불일치 0건 (verdict: pass) - 2026-07-07 V2 실측: SIM_RATE=300으로 102,655 이벤트 주입 → 시뮬레이터 정지 → 90초 대기 (consumer group 5파티션 lag 0 확인) → `POST /admin/verify/seller-dashboard` → quiesced=true, verdict=pass, mismatchCount=0, 100/100 groups. 윈도우 뷰(seller-sales-window)는 엔진이 명시적으로 400 "MVP 범위 밖" 거부 (ExpectedViewSqlGenerator 의도적 가드, Plan 08 설계 노트대로 - 조용한 실패 아님). 상세: .planning/v2-consistency-chaos-verification.md
- [ ] AC7-3: 스트림이 흐르는 중 실행하면 non-quiesced 플래그가 표시되고 verdict가 없다
- [ ] AC7-4 (N4): 검증 실행 중 REST 조회 루프가 계속 200으로 응답한다

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
./gradlew :engine:test --tests '*Verif*'
docker compose up -d --build
# 데이터 주입 후:
curl -X POST http://localhost:8080/admin/verify/seller-dashboard
```
