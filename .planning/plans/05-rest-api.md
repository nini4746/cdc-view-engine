# Plan 05: REST 조회 API

## 목표

state store에 구체화된 뷰를 REST로 조회하는 API (키 조회, 윈도우 범위 조회, ETag/freshness) 를 구현한다. SPEC R4 (AC4-1 ~ AC4-4) 를 구현한다.

## 컨텍스트

- 저장소: /Users/yunho/project/cdc-view-engine, SPEC: .planning/SPEC.md
- Plan 04: 뷰별 state store (`view-{name}-v{N}`, 윈도우 뷰는 WindowStore). KafkaStreams 인스턴스에서 `store()` 로 ReadOnlyKeyValueStore / ReadOnlyWindowStore 조회 가능 (단일 인스턴스 전제, 원격 라우팅 불필요)
- freshness 메타: 뷰 갱신 시 키별 (또는 뷰별) 마지막 반영 이벤트의 커밋 ts_ms와 반영 시각 (wall clock) 을 함께 저장해야 함 - Plan 04의 집계 값 객체에 lastCommitTs/lastAppliedAt/lastOffset 필드를 포함시키는 방식 권장 (Plan 04 산출물 수정 허용)
- Plan 09가 스왑을 위해 "현재 서빙 버전" 간접 참조를 쓸 예정 → 컨트롤러는 뷰명 → 현재 store 이름 해석을 ViewRegistry 경유로
- 코드 위치 제안: engine/src/main/java/io/github/nini4746/cdcview/api/ (ViewQueryController, ViewResponse, 예외 핸들러)

## 작업

1. GET /views/{view}/{key}: 집계값 + freshness (sourceCommitTs, appliedAt) JSON 반환
2. GET /views/{view}/{key}?from={epochMs}&to={epochMs}: 윈도우 뷰의 시간 범위 조회 (windowStore.fetch), 윈도우 start/end 포함 목록 반환. 윈도우 뷰에 range 파라미터 없으면 400
3. 404 처리: 미등록 뷰 / 없는 키 → 404 + `{ "error": ..., "view": ..., "key": ... }` 바디
4. ETag: `"{storeVersion}-{lastOffset}"` (또는 lastCommitTs) 기반 생성, If-None-Match 일치 시 304 (바디 없음)
5. Streams가 REBALANCING/NOT_RUNNING 상태일 때 503 반환 (500 금지)
6. 테스트: MockMvc + 테스트용 store 스텁, 또는 TopologyTestDriver 결합 통합 테스트. 304 시나리오 포함

## must-haves

- 응답 스키마 문서화 (README 또는 코드 주석 수준이라도 필드 의미 고정)
- 뷰명 → store 해석이 한 곳 (레지스트리) 에 집중 (Plan 09 스왑 대비)
- 존재하지 않는 리소스에 200 + null 반환 금지

## 검증

- [ ] AC4-1: GET /views/seller-dashboard/{sellerId} 가 최신 집계값 + freshness 메타 (마지막 반영 이벤트 커밋 ts_ms, 뷰 반영 시각) 를 JSON으로 반환한다
- [ ] AC4-2: GET /views/seller-sales-window/{sellerId}?from=..&to=.. 가 범위 내 윈도우 결과 목록을 반환한다
- [ ] AC4-3: 없는 키와 없는 뷰 각각 404 + 뷰명/키가 담긴 에러 바디를 반환한다
- [ ] AC4-4: 응답에 ETag가 포함되고, 같은 ETag로 If-None-Match 요청 시 304를 반환한다

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
./gradlew :engine:test --tests '*Api*'
docker compose up -d --build
curl -i http://localhost:8080/views/seller-dashboard/1
curl -i -H 'If-None-Match: "<etag>"' http://localhost:8080/views/seller-dashboard/1
```
