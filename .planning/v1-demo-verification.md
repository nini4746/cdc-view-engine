# V1 검증 리포트: 데모/헬스 재현 (SPEC R10)

- 일시: 2026-07-07 21:24 ~ 21:47 (KST)
- 환경: macOS (Darwin 25.5.0), Docker Desktop (server 29.4.3), 클린 이미지 빌드 (엔진/시뮬레이터 로컬 빌드 + postgres:16, apache/kafka:3.9.0, debezium/connect:3.0.0.Final 신규 pull)
- 범위: docker compose 기동 → scripts/demo-check.sh → AC10-1 ~ AC10-5 실측 + /admin/verify 헬스 수준 확인. V2 (카오스/정합성 본검증), V3 (벤치) 는 범위 외.

## 환경 특이사항 (프로젝트 결함 아님)

- 호스트 8080 포트를 외부 프로세스 (`/Users/yunho/project/streaming-site` 의 Spring Boot 앱, PID 85563, 검증 도중 기동됨) 가 점유. 해당 스택을 건드리지 않고 **호스트 포트만 18080으로 매핑하는 compose override** (커밋하지 않음, 스크래치 파일) 로 우회. 컨테이너 내부 포트/구성은 docker-compose.yml 그대로.
- 이 때문에 demo-check.sh (REST가 localhost:8080 하드코딩) 는 1차 실행에서 엔진 포트 바인딩 실패로 exit 1. 이후 검증은 동일 로직을 18080 대상으로 재현해 실측.
- 검증 도중 Docker Desktop 데몬이 일시적으로 죽어 (21:33경) 재기동 후 계속 진행. 결과에 영향 없음.

## AC10 실측 결과

| AC | 내용 | 결과 | 실측 |
|---|---|---|---|
| AC10-1 | PG/Kafka/Connect/엔진 모두 healthy | PASS | compose up (이미지 빌드 완료 상태) 기점 **+34초**에 4개 서비스 전부 healthy |
| AC10-2 | actuator health 200 | PASS | +34초에 HTTP 200, `status: UP` (db/ping/diskSpace UP) |
| AC10-3 | 시뮬레이터 compose 동반 구동 + 설정 가능 rate | PASS (D1 수정 후) | +34초 기동, `rate=100 sellers=100` 로그. achieved 75~78 events/sec (아래 관찰 사항) |
| AC10-4 | up 후 5분 내 수동 개입 없이 뷰 실데이터 | **FAIL** | +300초까지 `{"error":"Streams가 RUNNING 상태가 아닙니다"}`. 원인 = 결함 E1. 엔진 재시작 워크어라운드 후 **+7초** 만에 실데이터 반환 |
| AC10-5 | README 절차 (명령어 3개) 그대로 재현 | **FAIL (블록)** | AC10-4에 종속. E1 해결 전 재현 불가 |

워크어라운드 후 실데이터 예시 (seller 1):

```json
{"view":"seller-dashboard","key":"1","values":{"total_order_amount":24734.0,"order_count":95,"total_settled_amount":4841.0,"min_inventory":953.0},"freshness":{"sourceCommitTs":1783485839104,"appliedAt":1783485860854},"version":1}
```

윈도우 뷰도 정상: `GET /views/seller-sales-window/1?from=…&to=…` → 최근 15분 범위에서 윈도우 14건 반환.

## demo-check.sh 실행 요지

- 1차 (원본 포트 8080): compose 빌드/기동 진행 중 `Error response from daemon: ports are not available: … 0.0.0.0:8080 … address already in use` 로 exit 1 (외부 포트 충돌, 스크립트 자체 결함 아님). 소요 2분 33초 (이미지 pull 포함).
- 2차 (동일 로직을 18080으로 재현): 엔진 health UP까지는 통과, 뷰 데이터 대기 단계에서 5분 타임아웃 → AC10-4 FAIL과 동일 원인 (E1).

## 발견 결함과 처리

### D1 (데모 글루, 수정함): 시뮬레이터 SIM_SEED 빈 문자열 크래시

- 증상: `simulator-1 | Exception in thread "main" java.lang.NumberFormatException: For input string: ""` (Simulator.java:41) → 컨테이너 Exited(1), 데이터 생성 0건.
- 원인: docker-compose.yml 이 `SIM_SEED: ${SIM_SEED:-}` 로 **빈 문자열**을 넘기는데, Simulator 가 `getenv != null` 만 검사하고 `Long.parseLong("")` 수행.
- 처리: simulator/src/main/java/io/github/nini4746/cdcview/simulator/Simulator.java 의 env 헬퍼가 빈 문자열을 미설정으로 취급하도록 수정 (이번 커밋 포함). 수정 후 시뮬레이터 정상 구동 확인.

### E1 (엔진 코드, 수정하지 않고 목록화): Streams 기동 시 CDC 토픽 부재 → 영구 ERROR

- 증상: 엔진 기동 ~5초 후 `MissingSourceTopicException: Missing source topics. [cdcview.public.orders, cdcview.public.inventory, cdcview.public.settlements]` → 기본 예외 핸들러가 SHUTDOWN_CLIENT → Streams `PENDING_ERROR → ERROR` 로 영구 정지. 이후 뷰 API 는 503 성격의 `Streams가 RUNNING 상태가 아닙니다` 만 반환.
- 메커니즘 (클린 기동에서 결정적으로 재현):
  1. ConnectorRegistrar(@Order 10) 가 Debezium 커넥터 등록 → StreamsManager(@Order 20) 가 **즉시** Streams 기동.
  2. 이 시점 소스 테이블은 빈 상태 (시뮬레이터는 engine healthy 이후에야 시작). Debezium 은 빈 테이블 스냅샷에서 레코드를 내지 않으므로 CDC 토픽이 생성되지 않음 (커넥터/태스크는 RUNNING 인데 토픽 0개인 것 확인).
  3. Streams 첫 리밸런스가 INCOMPLETE_SOURCE_TOPIC_METADATA 로 실패 → 클라이언트 종료. 시뮬레이터가 데이터를 넣어 토픽이 생겨도 (실측: 이후 `cdcview.public.*` 3개 생성됨) Streams 는 복구되지 않음.
- 근거 실측: 클린 up 에서 엔진 로그 MissingSourceTopicException 3회. 토픽 생성 이후 엔진 컨테이너만 재시작하면 7초 만에 뷰 실데이터 반환 → 토폴로지/조인/집계/REST 는 전부 정상이고 기동 순서 경합만이 원커맨드 데모를 막음.
- 수정 방향 제안 (다음 plan, 엔진 코드): (a) Streams 기동 전 AdminClient 로 CDC 토픽 선생성 또는 존재 대기, (b) StreamsUncaughtExceptionHandler 로 MissingSourceTopicException 시 재시도, (c) Debezium topic.creation.default.* 설정으로 커넥터 등록 시 토픽 생성. (a)+(b) 조합 권장.

### 관찰 (결함 후보, V1 판정 아님)

- O1: 시뮬레이터 achieved rate 75~78/100 events/sec 로 목표 대비 ~25% 미달 (단일 커넥션 배치 특성 추정). AC10-3 의 "설정 가능한 속도" 자체는 충족하나 Plan 11 (10,000 events/sec) 전에 원인 확인 필요.
- O2: demo-check.sh 의 REST 주소가 localhost:8080 하드코딩. 포트 충돌 환경 대비 `REST` 환경변수 오버라이드 허용하면 편의성 개선 (V1에서는 수정하지 않음).

## /admin/verify 헬스 스냅샷 (V2 예고편, 헬스 수준)

- 스트림 흐르는 중 (시뮬레이터 가동): `{"view":"seller-dashboard","quiesced":false,"verdict":null,"mismatchCount":39,"expectedGroups":100,"actualGroups":100,"durationMs":36}` → AC7-3 대로 non-quiesced 플래그 + verdict 없음.
- 시뮬레이터 정지 + 60초 대기 (quiesce): `{"view":"seller-dashboard","quiesced":true,"verdict":"pass","mismatchCount":0,"expectedGroups":100,"actualGroups":100,"durationMs":39}` → 100 그룹 전부 일치.

## V1 판정

- AC10-1, AC10-2, AC10-3: 확정 (체크박스 반영).
- AC10-4, AC10-5: FAIL - 엔진 결함 E1 해결 후 재검증 필요. E1 은 기동 순서 문제로 국한되며, 데이터 경로 (CDC → 조인/집계 → REST) 와 정합성은 워크어라운드 하에 건강함을 확인.

## V2 (카오스/정합성) 착수 시 주의점

1. E1 을 먼저 고치지 않으면 V2 의 kill -9 재시작 시나리오 (chaos-restart-test.sh) 가 "재시작 후 토픽은 이미 존재" 조건이라 오히려 통과할 수 있다 - 클린 기동 경로와 재시작 경로를 구분해 판정할 것.
2. 검증 중 quiesce 대기: 시뮬레이터 정지 후 lag 0 도달까지 수십 초 걸림. 이번 실측은 60초 대기로 quiesced=true 획득. AC7-2 (10만 이벤트) 는 rate 미달 (O1) 상태로는 세팅 시간이 길어지니 O1 확인 후 진행 권장.
3. 호스트 8080 충돌이 재발할 수 있음 (streaming-site 스택). V2 스크립트는 REST 주소를 환경변수로 받게 하거나 검증 전 포트 점유 확인 필요.
4. demo-check.sh 는 `set -e` 라 compose up 실패 시 진단 없이 종료. 카오스 스크립트에서는 실패 시 `docker compose logs` 덤프를 남기는 편이 조사 비용을 줄임.
