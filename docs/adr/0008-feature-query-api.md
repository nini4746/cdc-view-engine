# 0008. 뷰 YAML의 features 선언으로 피처 조회 API 제공

- 상태: 채택
- 날짜: 2026-11-02

## 맥락
`ticket-fraud-engine`의 판정 엔진이 계정/카드 등 주체별 윈도우 피처를 조회해야 합니다.
판정 엔진은 제공되지 않는 피처가 있어도 해당 룰만 건너뛰고 판정을 이어가야 합니다.
distinct 계정 수 같은 피처를 위해 count_distinct 집계도 필요했습니다.

## 결정
- `POST /api/v1/features/query`를 추가했습니다. 뷰 YAML에 `features`를 선언하면 집계 필드가 `subject.name.window` 키로 노출됩니다.
- 선언은 부팅 시 검증합니다. 잘못된 선언은 파일명 + 필드 경로로 에러를 냅니다.
- 제공할 수 없는 피처는 에러가 아니라 응답의 `missing` 목록으로 돌려줍니다.
- 값은 지금을 포함하는 윈도우 중 가장 오래된 것에서 읽습니다.
- 빈 구간에서 count/count_distinct/sum은 0, min/max/avg는 값 없음입니다.
- count_distinct는 값별 행 수 맵으로 유지하고, SQL `COUNT(DISTINCT)`처럼 NULL은 세지 않습니다.
- 예시 뷰는 `examples/ticket-fraud-views/`에 있습니다.

## 대안
- 요청 시 뷰/필드를 동적으로 해석: 잘못된 선언이 조회 시점에야 드러납니다.
- 제공 불가 피처를 404/400 에러로: 판정 엔진이 해당 룰만 건너뛸 수 없습니다.
- 가장 최근 윈도우에서 읽기: 최근 구간을 덜 덮습니다.
- count_distinct를 값 집합이나 근사 알고리즘으로: UPDATE와 DELETE를 정확히 되돌릴 수 없습니다.

## 결과
- 장점: 판정 엔진이 피처 단위로 결측을 처리합니다. count_distinct가 UPDATE/DELETE에도 정확합니다.
- 단점/감수한 점: sliding 뷰의 정밀도는 advance 단위입니다. count_distinct 저장 공간이 고유 값 수에 비례합니다.
