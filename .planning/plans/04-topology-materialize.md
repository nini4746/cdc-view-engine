# Plan 04: Kafka Streams 토폴로지 - 조인/집계 구체화

## 목표

뷰 정의로부터 Kafka Streams 토폴로지 (조인, 집계, 윈도우 집계) 를 동적으로 구성해 state store에 뷰를 구체화한다. SPEC R3 (AC3-1 ~ AC3-4) 을 구현한다.

## 컨텍스트

- 저장소: /Users/yunho/project/cdc-view-engine, SPEC: .planning/SPEC.md
- Plan 02: ViewDefinition/ViewRegistry (sources, 조인 키, groupBy, aggregations count/sum/min/max/avg, 윈도우 tumbling/sliding + grace)
- Plan 03: CDC 토픽 + ChangeEvent 파서 (before/after/op/lsn/ts_ms)
- 설계 방향: CDC 토픽을 KTable화 (키 = PK) 후 조인 키로 리파티션, foreign-key join 또는 groupBy-aggregate로 조인. 집계는 ChangeEvent의 before/after를 이용한 가산/차감 (UPDATE = old 차감 + new 가산, DELETE = 차감). 윈도우 집계는 event time = source.ts_ms 기준 TimestampExtractor 사용, [start, end) 반개구간 (Kafka Streams 기본과 일치 여부를 테스트로 고정)
- state store 네이밍 규칙 필요 (Plan 05 REST 조회, Plan 09 재구축 스왑에서 참조): `view-{viewName}-v{N}` 형태 권장
- 코드 위치 제안: engine/src/main/java/io/github/nini4746/cdcview/topology/ (TopologyBuilder, AggregatorFactory, EventTimeExtractor)

## 작업

1. EventTimeExtractor: ChangeEvent의 source.ts_ms를 event time으로 사용
2. AggregatorFactory: count/sum/min/max/avg 각각의 add/subtract 로직 (avg는 sum+count 쌍으로 유지, min/max는 감산 불가하므로 그룹 내 재계산 전략 또는 소스 행 상태 유지 방식 명시적으로 선택해 구현)
3. TopologyBuilder: ViewDefinition 1개 → 서브토폴로지 1개
   - 윈도우 없는 뷰: 소스별 KTable → 조인 키 리파티션 → 조인 → groupBy(groupBy 키) → aggregate (Materialized as `view-{name}-v1`)
   - 윈도우 뷰: KStream → groupByKey(groupBy 키) → windowedBy (tumbling: TimeWindows.ofSizeAndGrace / sliding(hopping): advanceBy) → aggregate (WindowStore)
4. Streams 라이프사이클 관리 (Spring 통합, 종료 훅, 상태 리스너)
5. TopologyTestDriver 단위 테스트: INSERT 반영, UPDATE 차감/가산, DELETE 제거, 윈도우 경계 (end 시각 이벤트가 다음 윈도우에 속함)
6. 통합 확인: 데모 뷰 2개 (Plan 02의 YAML) 가 실제 compose 환경에서 구체화되는지 state store 조회 로그로 확인

## must-haves

- 뷰 정의만으로 토폴로지 생성 (뷰별 하드코딩 금지)
- UPDATE에서 before 기반 차감 누락 금지 (sum 이중 가산 방지)
- state store 이름 규칙이 버전 접미사 (v{N}) 를 포함 (Plan 09 스왑 대비)
- event time은 반드시 커밋 ts_ms (처리 시각 아님)

## 검증

- [ ] AC3-1: 주문 INSERT 후 조인 뷰의 해당 seller_id 집계값이 갱신된다 (TopologyTestDriver + compose 확인)
- [ ] AC3-2: UPDATE 시 sum 집계에서 이전 값이 차감되고 새 값이 가산된다 (예: 100→150 UPDATE 후 sum 증가분이 정확히 50)
- [ ] AC3-3: DELETE 시 해당 행 기여분이 집계에서 제거된다
- [ ] AC3-4: 윈도우 종료 시각과 정확히 같은 event time의 이벤트는 다음 윈도우에 집계된다 (TopologyTestDriver 테스트)

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
./gradlew :engine:test --tests '*Topology*'
```
