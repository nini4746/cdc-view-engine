# Plan 03: Debezium 커넥터 자동 등록 + CDC 이벤트 수집

## 목표

엔진이 뷰 정의에 선언된 테이블의 Debezium 커넥터를 자동 등록하고, CDC 토픽을 소비해 내부 이벤트로 파싱한다. SPEC R2 (AC2-1, AC2-2, AC2-3) 를 구현한다.

## 컨텍스트

- 저장소: /Users/yunho/project/cdc-view-engine, SPEC: .planning/SPEC.md
- Plan 01: compose (postgres wal_level=logical, kafka, connect(Debezium), engine), 데모 테이블 orders/inventory/settlements (REPLICA IDENTITY FULL)
- Plan 02: ViewRegistry - 뷰 정의에서 소스 테이블 목록 조회 가능
- Debezium PG 커넥터는 Kafka Connect REST API (connect:8083) 로 등록. 토픽 네이밍: {prefix}.{schema}.{table}
- 내부 이벤트 모델에 필요한 필드: 테이블명, op (c/u/d/r), before, after, source.lsn, source.ts_ms (커밋 시각)
- 코드 위치 제안: engine/src/main/java/io/github/nini4746/cdcview/cdc/ (ConnectorRegistrar, ChangeEvent, ChangeEventDeserializer)

## 작업

1. ConnectorRegistrar: 부팅 시 ViewRegistry의 소스 테이블 합집합으로 Debezium PG 커넥터 설정 생성 → Connect REST API로 등록 (이미 있으면 설정 갱신, 멱등). 커넥터 설정: plugin.name=pgoutput, snapshot.mode=initial, table.include.list, decimal 처리 방식 명시
2. 등록 전 사전 검사: 각 대상 테이블의 REPLICA IDENTITY를 PG 카탈로그 (pg_class.relreplident) 로 조회, FULL('f') 아니면 테이블명 포함 예외로 부팅 실패
3. ChangeEvent 모델 + Debezium JSON envelope 파서 (before/after/op/lsn/ts_ms 추출). 스냅샷 이벤트 (op=r) 도 처리
4. 수신 확인용 관측: 테이블별 수신 카운터 메트릭 (Micrometer) + DEBUG 로그
5. 통합 테스트 (Testcontainers: PG + Kafka + Connect 또는 Debezium embedded 대체 가능하나 실제 Connect 경로 권장): INSERT/UPDATE/DELETE 각 1건 → ChangeEvent 3건 파싱 확인 (lsn 단조 증가, UPDATE에 before 존재)

## must-haves

- 커넥터 등록이 멱등 (재부팅해도 중복 커넥터/에러 없음)
- UPDATE/DELETE 이벤트에서 before 이미지 접근 가능 (REPLICA IDENTITY FULL 전제)
- lsn과 커밋 ts_ms가 이후 plan (순서 처리, 지연 측정) 에서 쓸 수 있는 형태로 보존

## 검증

- [ ] AC2-1: compose 기동 후 `curl http://localhost:8083/connectors` 에 엔진이 등록한 커넥터가 존재하고, 수동 등록 절차가 없다
- [ ] AC2-2: 소스 테이블에 INSERT/UPDATE/DELETE 실행 시 before/after, LSN, 커밋 ts_ms를 파싱한 내부 이벤트 수신이 메트릭 (`curl localhost:8080/actuator/metrics/cdc.events.received`) 또는 로그로 확인된다
- [ ] AC2-3: REPLICA IDENTITY FULL이 아닌 테이블을 뷰 정의에 넣고 부팅하면 테이블명을 포함한 명시적 에러로 실패한다

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
./gradlew :engine:test --tests '*Cdc*'
docker compose up -d --build
curl http://localhost:8083/connectors
docker compose exec postgres psql -U app -d demo -c "INSERT INTO orders(...) VALUES (...);"
curl http://localhost:8080/actuator/metrics/cdc.events.received
```
