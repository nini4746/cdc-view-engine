# 0006. Debezium 폴링 간격 단축과 파티션/스트림 스레드 8개

- 상태: 채택
- 날짜: 2026-07-08

## 맥락
V3 벤치에서 두 가지가 목표(10,000/s, p95 1초 이하)를 막았습니다.
- 지연 바닥: 지속 가능한 1,000~2,000/s에서도 p50이 716ms로 고정되었습니다. Debezium 기본 `poll.interval.ms=500`의 배치 대기가 원인이었습니다.
- 처리량 포화: 10,000/s에서 단일 스트림 스레드가 따라가지 못해 lag이 831k까지 계속 증가했습니다.

## 결정
- Debezium `poll.interval.ms` 500 → 10, `max.batch.size` 2048 → 256 (T1).
- CDC 소스 토픽 파티션과 `num.stream.threads` 1 → 8 (T2).
- 값은 `application.yml`의 `CONNECT_POLL_INTERVAL_MS`, `CONNECT_MAX_BATCH_SIZE`, `CONNECT_SOURCE_PARTITIONS`, `KAFKA_STREAM_THREADS`로 조절할 수 있습니다.

## 대안
- Debezium 기본값 유지: p50이 부하와 무관하게 716ms로 고정됩니다.
- 단일 스레드 유지: 10,000/s에서 lag이 계속 늘어납니다.
- 파티션/스레드 4/4: lag이 느리게 줄지만 p95 22,906ms로 부족했습니다.

## 결과
- 장점: T1 후 1,000/s에서 p95 179ms. T2 후 10,000/s 10분 지속에서 p50 134ms, p95 537ms, lag bounded. 8파티션에서도 정합성 검증 mismatch 0.
- 단점/감수한 점: T2 최종 측정은 시뮬레이터 `SIM_MAX_BATCH`를 500 → 150으로 낮춰 부하 버스트를 평활화한 조건입니다. 스레드 수는 파티션 수 이하에서만 효과가 있습니다. 8/8이 측정 호스트(8코어)의 처리량 상한이고, 그 이상은 멀티 노드가 필요합니다.
