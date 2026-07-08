# Plan 10: 커머스 시뮬레이터 + 원커맨드 데모

## 목표

주문/재고/정산 데이터를 설정 가능한 속도로 생성하는 커머스 시뮬레이터를 만들고, docker compose 원커맨드 데모를 완성한다. SPEC R10의 잔여 기준 (AC10-3, AC10-4, AC10-5) 을 구현한다.

## 컨텍스트

- 저장소: /Users/yunho/project/cdc-view-engine, SPEC: .planning/SPEC.md
- Plan 01: compose (postgres/kafka/connect/engine, AC10-1/2는 이미 충족), 데모 테이블 orders/inventory/settlements, Gradle 멀티모듈 골격
- Plan 05: REST 조회 (GET /views/seller-dashboard/{sellerId})
- 시뮬레이터 요건: PG에 직접 INSERT/UPDATE/DELETE (CDC의 소스는 어디까지나 PG). 시나리오 - 주문 생성/상태 변경/취소, 재고 증감, 정산 생성. 판매자 N명 (기본 100), 속도 events/sec 환경변수 (`SIM_RATE`, 기본 100, Plan 11에서 10,000까지 올림)
- 별도 Gradle 모듈 simulator/ (경량 JDBC 앱, Spring 불필요 - plain Java + HikariCP 권장) + Dockerfile
- 코드 위치 제안: simulator/src/main/java/io/github/nini4746/cdcview/simulator/

## 작업

1. simulator 모듈: 시드 데이터 로딩 (판매자/상품), 이벤트 제너레이터 (INSERT/UPDATE/DELETE 혼합 비율 설정 가능), 목표 rate 유지 루프 (토큰 버킷), 실제 달성 rate 로그 출력
2. 결정성 옵션: `SIM_SEED` 지정 시 재현 가능한 시퀀스 (Plan 07/08 테스트에서 재사용 가능)
3. simulator/Dockerfile + compose에 simulator 서비스 추가 (engine healthy 이후 시작, `SIM_RATE`/`SIM_SEED`/`SIM_DURATION` 환경변수)
4. 데모 완성 확인: compose up → 시뮬레이터 데이터 생성 → seller-dashboard 뷰 조회 실데이터 확인
5. README 데모 섹션 작성: 명령어 3개 이하 (예: `docker compose up -d --build` → `curl .../views/seller-dashboard/1` → `docker compose down -v`). 장애 서사 (README 차별화 공식) 는 문서 몫이므로 최소한만 - 스킬 범위에서 데모 절차만 필수
6. 원커맨드 검증 스크립트 scripts/demo-check.sh: compose up부터 뷰 조회 성공까지 5분 타임아웃으로 자동 확인

## must-haves

- 시뮬레이터는 Kafka에 직접 쓰지 않음 (반드시 PG 경유 - CDC 경로 검증이 목적)
- rate가 환경변수로 조절 가능하고 달성 rate가 로그로 확인 가능 (Plan 11 전제)
- 데모가 수동 개입 (커넥터 수동 등록, SQL 수동 실행 등) 을 요구하지 않음

## 검증

- [x] AC10-3: 커머스 시뮬레이터가 compose로 함께 구동되어 주문/재고/정산 데이터를 설정 가능한 속도 (events/sec) 로 생성한다 - 2026-07-07 V1 실측: compose up +34초에 시뮬레이터 기동 (rate=100 sellers=100), rate 로그 출력 확인. 단 achieved 75~78/100 events/sec 로 목표 미달 (Plan 11 전 확인 필요). SIM_SEED 빈 문자열 크래시 결함 (D1) 수정 후 통과
- [ ] AC10-4: compose up 후 5분 내 수동 개입 없이 GET /views/seller-dashboard/{id} 가 실데이터를 반환한다 (demo-check.sh 통과) - 2026-07-07 V1 실측 FAIL: 엔진 Streams가 기동 시 CDC 토픽 부재로 MissingSourceTopicException → 영구 ERROR (결함 E1, 엔진 코드). 엔진 컨테이너 재시작 워크어라운드 후에는 +7초 만에 실데이터 반환 (파이프라인 자체는 정상). 상세: .planning/v1-demo-verification.md
- [ ] AC10-5: README 데모 절차 (명령어 3개 이하) 를 그대로 따라 재현 가능하다 (클린 체크아웃 기준으로 1회 실측) - 2026-07-07 V1: E1 해결 전까지 재현 불가 (AC10-4에 종속)

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
bash scripts/demo-check.sh
SIM_RATE=500 docker compose up -d --build && docker compose logs -f simulator
```
