# 0003. PG LSN으로 순서와 중복을 판정하고 EOS v2를 병행

- 상태: 채택
- 날짜: 2026-07-07

## 맥락
CDC 이벤트는 순서가 뒤집혀 도착할 수 있습니다. DELETE가 UPDATE보다 먼저 오면 행이 되살아날 위험이 있습니다.
Kafka Streams EOS는 Kafka 내부만 보장하고, Debezium → Kafka 구간은 at-least-once라 중복이 들어옵니다.
윈도우 grace를 넘긴 지각 이벤트를 Kafka Streams는 기본적으로 조용히 버립니다. SPEC은 흔적 없는 폐기를 금지합니다.

## 결정
- 순서 기준은 Debezium 이벤트의 PG LSN입니다. `LsnGuard`가 소스 행(`table|pk`)별 최고 LSN을 store에 유지하고, 그 이하 LSN 이벤트는 폐기하고 카운터를 올립니다(last-write-wins).
- 가드는 리키잉 전 소스 PK 단위로 `FixedKeyProcessor`로 적용합니다.
- `processing.guarantee=exactly_once_v2`를 켜고, 같은 LSN 중복은 위 가드가 걸러냅니다.
- event time은 커밋 시각(`ts_ms`)입니다. 윈도우는 [start, end) 반개구간이고 grace 기본 30초입니다.
- grace 초과 이벤트는 `LateEventHandler`가 `cdcview.dlq.late-events` 토픽으로 보내고 카운터를 올립니다.

## 대안
- Kafka 오프셋이나 `ts_ms`를 순서 기준으로: 원본 커밋 순서를 그대로 반영하지 않습니다.
- 그룹 키 단위 가드: 추가 repartition이 필요하고 행 단위 판정이 안 됩니다.
- EOS v2만 사용: Debezium → Kafka 구간 중복을 막지 못합니다.
- grace 초과를 Kafka Streams 기본 drop에 맡김: 버려진 흔적이 남지 않습니다.

## 결과
- 장점: 소스 토픽의 PK 파티셔닝을 그대로 써 추가 repartition이 없습니다. 같은 LSN 중복이 자연 차단됩니다. DELETE도 LSN을 갱신해 낮은 LSN의 UPDATE로 행이 되살아나지 않습니다.
- 단점/감수한 점: DELETE 이후의 LSN 기록이 store에 남고, TTL 정리는 MVP 범위 밖입니다.
