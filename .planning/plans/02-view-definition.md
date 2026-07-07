# Plan 02: YAML 뷰 정의 로딩과 검증

## 목표

YAML 뷰 정의 파일을 부팅 시 로딩/검증해 뷰 레지스트리를 구성한다. SPEC R1 (AC1-1, AC1-2, AC1-3) 을 구현한다.

## 컨텍스트

- 저장소: /Users/yunho/project/cdc-view-engine, SPEC: .planning/SPEC.md
- Plan 01이 만든 것: Gradle 멀티모듈 (engine/), Spring Boot 골격, docker compose (PG orders/inventory/settlements 테이블), 베이스 패키지 io.github.nini4746.cdcview
- 뷰 정의 위치: 저장소 루트 `views/*.yaml` (컨테이너에는 볼륨 마운트 또는 이미지 포함)
- 지원 스펙: 소스 테이블 목록, 조인 키 (테이블별 컬럼 매핑), 집계 함수 count/sum/min/max/avg (대상 컬럼 + 결과 필드명), 그룹 키, 윈도우 (type: tumbling|sliding, size, advance(sliding만), grace 기본 30s), 윈도우 없는 뷰도 허용
- 코드 위치 제안: engine/src/main/java/io/github/nini4746/cdcview/config/ (ViewDefinition, ViewDefinitionLoader, ViewRegistry, 검증 로직)

## 작업

1. ViewDefinition 모델 정의 (Jackson YAML 역직렬화, `FAIL_ON_UNKNOWN_PROPERTIES` 활성)
2. 로더: 설정된 디렉토리의 *.yaml 전체 로딩, 파일 단위 파싱
3. 검증 규칙: 필수 필드 (name, sources, groupBy, aggregations), 조인 키 정합 (2개 이상 소스면 조인 키 필수), 집계 함수 화이트리스트, 윈도우 필드 조합 (sliding이면 advance 필수), 뷰 이름 중복 금지
4. 검증 실패 시: 파일명 + 필드 경로 + 사유를 담은 예외 → 부팅 실패 (Spring FailureAnalyzer 또는 ApplicationRunner에서 throw), exit code != 0
5. 데모 뷰 정의 2개 작성:
   - views/seller-dashboard.yaml: orders + inventory + settlements 조인, seller_id 그룹, sum(주문액)/count(주문)/sum(정산액)/min(재고) 집계, 윈도우 없음
   - views/seller-sales-window.yaml: orders 단일 소스, seller_id 그룹, sliding 윈도우 (size 10m, advance 1m, grace 30s), count + sum
6. 단위 테스트: 정상 로딩, 필수 필드 누락, 알 수 없는 필드, sliding에 advance 누락, 이름 중복

## must-haves

- 코드 수정 없이 YAML 추가만으로 뷰가 레지스트리에 등록되는 구조
- 알 수 없는 필드도 에러 (오타 침묵 통과 금지)
- 에러 메시지에 파일명과 필드 경로 포함

## 검증

- [ ] AC1-1: 소스 2개 이상 + 조인 키 + 집계 함수 + 윈도우를 선언한 YAML이 부팅 시 등록된다 (테스트로 확인)
- [ ] AC1-2: 필수 필드 누락/알 수 없는 필드 YAML로 부팅하면 실패하고 (exit code != 0) 에러 메시지에 파일명과 필드 경로가 있다
- [ ] AC1-3: views/seller-dashboard.yaml, views/seller-sales-window.yaml 2개가 저장소에 존재하고 정상 로딩된다

테스트 명령어:
```bash
cd /Users/yunho/project/cdc-view-engine
./gradlew :engine:test --tests '*ViewDefinition*'
# 부팅 실패 확인: 깨진 YAML을 views 디렉토리로 지정해 실행 후 exit code 확인
```
