# 0002. 소스별 스트림을 그룹 키로 병합하는 증분 집계

- 상태: 채택
- 날짜: 2026-07-07

## 맥락
뷰는 여러 소스 테이블(주문, 재고, 정산)을 그룹 키로 묶어 count/sum/min/max/avg를 냅니다.
UPDATE와 DELETE가 들어와도 원본을 다시 읽지 않고 집계를 맞춰야 합니다.
Plan 04 설계 방향은 KTable 조인이었지만, 구현은 다른 방식을 택했습니다.

## 결정
- `TopologyBuilder`가 소스 테이블별 CDC 스트림을 그룹 키로 리키잉해 병합한 뒤 하나의 aggregate로 뷰를 만듭니다.
- `AggregateState`가 before/after 이미지로 증분 계산합니다. INSERT/READ는 가산, UPDATE는 before 감산 + after 가산, DELETE는 before 감산입니다.
- sum/count/avg는 O(1) 증분입니다. MIN/MAX는 값 멀티셋(값 -> 개수)으로 유지합니다.
- before 이미지를 얻기 위해 대상 테이블에 REPLICA IDENTITY FULL을 요구합니다.

## 대안
- KTable 행 조인: 조인 팬아웃이 생깁니다.
- MIN/MAX를 최솟값/최댓값 하나만 저장: 현재 최솟값이 DELETE/UPDATE로 빠지면 원본 재조회 없이 다시 계산할 수 없습니다.

## 결과
- 장점: 조인 팬아웃이 없습니다. 정합성 검증기 SQL도 테이블별 `GROUP BY` 후 병합으로 같은 의미를 재현합니다.
- 단점/감수한 점: MIN/MAX 상태 크기가 고유 값 수에 비례합니다. 그룹 키가 바뀌는 UPDATE(예: 주문의 seller_id 변경)는 MVP 범위 밖입니다.
