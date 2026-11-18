# 0001. 뷰 상태를 Kafka Streams state store에 두고 같은 프로세스에서 서빙

- 상태: 채택
- 날짜: 2026-07-07

## 맥락
CDC로 구체화한 집계 뷰를 어디에 저장하고 어떻게 조회할지 정해야 했습니다.
뷰 상태는 장애 후 복원되어야 하고, exactly-once 처리 범위 안에 들어가야 합니다.
1인 개발이라 운영할 저장소를 늘리고 싶지 않았습니다.

## 결정
- 뷰 상태를 Kafka Streams state store(RocksDB)에 저장합니다. store 이름은 `view-{name}-v{N}`입니다.
- REST API는 같은 프로세스에서 interactive query로 store를 읽습니다.
- 단일 인스턴스를 전제로 합니다.

## 대안
- 외부 DB나 Redis에 결과 저장: 별도 저장소가 늘고, 처리 결과와 저장이 한 트랜잭션에 묶이지 않습니다.

## 결과
- 장점: store가 changelog로 복원되고 EOS 트랜잭션에 포함됩니다. 별도 저장소 없이 서빙합니다.
- 단점/감수한 점: 멀티 인스턴스 스케일아웃은 범위 밖입니다. interactive query 라우팅은 별도 프로젝트급 작업이라 다루지 않았습니다.
