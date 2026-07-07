# Plan 09: 스키마 변경 중 무중단 뷰 재구축

## 목표

소스 스키마 변경 (컬럼 추가) 을 무중단으로 수용하고, 뷰 정의 변경 시 섀도 빌드 + 원자적 스왑으로 뷰를 재구축한다. SPEC R8 (AC8-1 ~ AC8-6, 부정 기준 N2 포함) 을 구현한다.

## 컨텍스트

- 저장소: /Users/yunho/project/cdc-view-engine, SPEC: .planning/SPEC.md
- Plan 04: state store 버전 접미사 규칙 `view-{name}-v{N}`. Plan 05: 뷰명 → 현재 store 해석이 레지스트리에 집중됨 (스왑 지점). Plan 08: 정합성 검증기 (재구축 무유실 검증에 사용)
- 설계 방향:
  - 컬럼 추가 내성: Debezium pgoutput은 컬럼 추가를 이벤트 스키마에 자동 반영. 엔진 파서 (Plan 03) 가 뷰 미참조 필드를 무시하도록 되어 있는지 확인/보강
  - 재구축 파이프라인: POST /admin/views/{view}/rebuild → (1) 뷰 정의 재로딩 + 검증 (참조 컬럼이 PG information_schema에 존재하는지 확인, 없으면 400 + 컬럼명) → (2) Debezium incremental snapshot (signal 테이블) 또는 신규 스냅샷 커넥터로 재스냅샷 트리거 → (3) 새 버전 store `view-{name}-v{N+1}` 에 섀도 집계 (신규 CDC 이벤트도 섀도에 함께 적용) → (4) 섀도가 라이브 오프셋을 따라잡으면 (lag 임계 이하) 레지스트리 포인터를 원자적으로 v{N+1} 로 교체 → (5) 구버전 store 정리
  - 구현 참고: Kafka Streams 토폴로지의 동적 교체는 불가하므로, 재구축용 별도 KafkaStreams 인스턴스 (다른 application.id) 를 띄워 섀도 store를 만들고 스왑하는 방식 권장. 스왑은 레지스트리의 volatile 참조 교체 = 원자적
  - 재시작 멱등: 재구축 상태를 내부 토픽 또는 로컬 파일에 기록하지 않는 단순 전략 - 재시작 시 미완의 섀도는 버리고 (구버전 서빙 유지), rebuild 재요청 시 처음부터. "재개" 는 범위 외
- 코드 위치 제안: engine/src/main/java/io/github/nini4746/cdcview/rebuild/ (RebuildService, RebuildController, ShadowStreamsFactory)

## 작업

1. 컬럼 추가 내성 테스트: ALTER TABLE ADD COLUMN 후 INSERT/UPDATE 지속 처리 확인 (통합 테스트)
2. 참조 컬럼 존재 검증 (information_schema 대조) - rebuild 요청 시 선행 실행
3. RebuildService: 재스냅샷 트리거 → 섀도 KafkaStreams 기동 → 따라잡기 감시 → 원자적 스왑 → 구버전 정리. 진행 상태 조회 GET /admin/views/{view}/rebuild/status
4. 스왑 무결성: 스왑 전후 REST 응답이 항상 단일 버전에서만 나오도록 (요청 시작 시점에 store 참조 1회 캡처)
5. 재구축 중 조회 연속성 테스트: rebuild 실행 동안 REST 폴링 루프 (예: 50ms 간격) → 5xx/빈 응답/버전 혼합 0건 검증. 혼합 검출 방법: 뷰 정의 변경으로 신구 응답 스키마 차이를 만들어 판별
6. 재구축 중 신규 이벤트 무유실: rebuild 진행 중 PG에 이벤트 계속 커밋 → 스왑 후 정합성 검증기 (Plan 08) 호출 → 불일치 0
7. 재시작 멱등 테스트: rebuild 중 엔진 재시작 → 기존 뷰 조회 정상 → rebuild 재요청 정상 완료
8. 참조 컬럼 삭제 거부 테스트: 컬럼 DROP 후 rebuild 요청 → 400 + 컬럼명 포함 에러

## must-haves

- 스왑은 단일 참조 교체 (다단계 부분 스왑 금지)
- 재구축 실패/중단 시 기존 뷰 서빙 무영향
- 섀도 빌드 리소스 (별도 streams 인스턴스, 임시 토픽/커넥터) 정리 누락 금지

## 검증

- [ ] AC8-1: 뷰 미참조 컬럼 ADD COLUMN 후에도 CDC 처리와 뷰 갱신이 에러 없이 지속된다
- [ ] AC8-2: 뷰 정의 변경 후 POST /admin/views/{view}/rebuild → 스냅샷부터 섀도 재구축, 완료 시 원자적 스왑
- [ ] AC8-3 (N2): 재구축 시작부터 스왑 완료까지 REST 폴링에서 5xx/빈 응답/신구 혼합 응답 0건
- [ ] AC8-4: 재구축 중 발생한 신규 CDC 이벤트가 스왑 후 뷰에 반영 - 스왑 후 정합성 검증 불일치 0건
- [ ] AC8-5: 뷰 참조 컬럼이 삭제된 상태의 rebuild 요청은 컬럼명 포함 명시적 에러로 거부된다
- [ ] AC8-6: 재구축 도중 엔진 재시작 → 기존 뷰 서빙 유지 + rebuild 재요청 정상 완료

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
./gradlew :engine:test --tests '*Rebuild*'
docker compose up -d --build
curl -X POST http://localhost:8080/admin/views/seller-dashboard/rebuild
curl http://localhost:8080/admin/views/seller-dashboard/rebuild/status
```
