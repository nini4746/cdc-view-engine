# 설계 결정 기록 (ADR)

cdc-view-engine의 구조를 정한 결정을 기록합니다. 요약 표는 [SDS 4. 설계 근거](../SDS.md#4-설계-근거)에 있습니다.

| 번호 | 결정 | 상태 | 날짜 |
|---|---|---|---|
| [0001](0001-kafka-streams-state-store-serving.md) | 뷰 상태를 Kafka Streams state store에 두고 같은 프로세스에서 서빙 | 채택 | 2026-07-07 |
| [0002](0002-incremental-aggregation-by-source-merge.md) | 소스별 스트림을 그룹 키로 병합하는 증분 집계 | 채택 | 2026-07-07 |
| [0003](0003-lsn-ordering-and-exactly-once.md) | PG LSN으로 순서와 중복을 판정하고 EOS v2를 병행 | 채택 | 2026-07-07 |
| [0004](0004-shadow-rebuild-and-version-swap.md) | 섀도 KafkaStreams로 재구축하고 서빙 버전 하나만 교체 | 채택 | 2026-07-07 |
| [0005](0005-rebuild-state-in-memory.md) | 재구축 진행 상태를 메모리에만 보관 | 대체됨 (0007) | 2026-07-07 |
| [0006](0006-debezium-polling-and-parallelism-tuning.md) | Debezium 폴링 간격 단축과 파티션/스트림 스레드 8개 | 채택 | 2026-07-08 |
| [0007](0007-persist-rebuild-state-in-postgres.md) | 재구축 상태를 소스 PostgreSQL에 영속화 | 채택 | 2026-07-13 |
| [0008](0008-feature-query-api.md) | 뷰 YAML의 features 선언으로 피처 조회 API 제공 | 채택 | 2026-11-02 |
