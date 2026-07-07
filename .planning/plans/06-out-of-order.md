# Plan 06: 순서 뒤집힌 이벤트 처리

## 목표

같은 키의 이벤트가 커밋 순서와 다르게 도착해도 최종 뷰 값이 LSN 순 처리 결과와 같도록 만든다. SPEC R5 (AC5-1 ~ AC5-4, 부정 기준 N3 포함) 를 구현한다.

## 컨텍스트

- 저장소: /Users/yunho/project/cdc-view-engine, SPEC: .planning/SPEC.md
- Plan 03: ChangeEvent (lsn, ts_ms 보유). Plan 04: 토폴로지 (KTable 조인 + 윈도우 집계, event time = 커밋 ts_ms, grace 지원)
- 설계 방향:
  - 비윈도우 경로: 소스 행 상태 store에 키별 최고 LSN을 유지, 도착 이벤트의 LSN이 저장된 LSN 이하이면 stale로 폐기 (Processor API 커스텀 프로세서를 KTable화 앞단에 삽입). DELETE 후 도착하는 낮은 LSN의 UPDATE도 이 규칙으로 차단 (tombstone에 LSN 보존 필요 - 삭제 마커에 마지막 LSN 기록)
  - 윈도우 경로: Kafka Streams grace 메커니즘 사용. grace는 뷰 정의 YAML 값 (기본 30초, Plan 02에서 이미 파싱)
  - grace 초과 지각 이벤트: 폐기 전 가로채서 DLQ 토픽 (`cdcview.dlq.late-events`) 에 원본 + 사유 적재, Micrometer 카운터 `view.late.events` 증가. Kafka Streams 기본 drop 메트릭에만 의존하지 말고 명시 처리
- 테스트 하네스: CDC 토픽에 임의 순서로 이벤트를 직접 produce하는 테스트 유틸 (Debezium을 거치지 않고 envelope 포맷으로 주입)
- 코드 위치 제안: engine/src/main/java/io/github/nini4746/cdcview/ordering/ (LsnGuardProcessor, LateEventHandler)

## 작업

1. LsnGuardProcessor: 키별 최고 LSN state store, stale 이벤트 폐기 + `view.stale.dropped` 카운터. DELETE tombstone에도 LSN 유지 (TTL 정리 전략 주석으로 명시)
2. 토폴로지에 프로세서 삽입 (Plan 04 TopologyBuilder 수정)
3. LateEventHandler: 윈도우 grace 초과 이벤트 감지 시 DLQ produce + 카운터. 구현 방식: 집계 전 event time과 stream time 비교하는 프로세서로 감지 (grace 값은 뷰 정의에서)
4. 역순 주입 테스트 유틸 (Debezium envelope 생성기) - Plan 07/11도 재사용
5. TopologyTestDriver 테스트: UPDATE 역순, DELETE-UPDATE 역순 (행 부활 금지), grace 내 지각 반영, grace 초과 DLQ

## must-haves

- stale 폐기와 grace 초과 폐기 모두 카운터로 관측 가능 (침묵 폐기 금지)
- DELETE 후 낮은 LSN 이벤트로 행이 부활하지 않음
- grace 값이 뷰 정의별로 다르게 적용됨

## 검증

- [ ] AC5-1: 같은 키 UPDATE 2건을 역순 주입 → 뷰 값이 LSN 큰 이벤트 기준
- [ ] AC5-2: DELETE가 UPDATE보다 먼저 도착하는 역순 시나리오 → 최종 뷰에 행이 되살아나지 않음
- [ ] AC5-3: grace (기본 30초, 뷰 정의로 설정 가능) 이내 지각 이벤트가 해당 윈도우 결과에 반영됨
- [ ] AC5-4 (N3): grace 초과 지각 이벤트는 뷰 미반영 + DLQ 토픽 적재 + late-event 카운터 증가 - 세 가지 모두 확인

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
./gradlew :engine:test --tests '*Ordering*' --tests '*LateEvent*'
```
