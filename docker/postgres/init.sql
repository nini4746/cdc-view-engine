-- 데모 커머스 스키마: 주문/재고/정산
-- 대상 테이블은 UPDATE/DELETE before 이미지 확보를 위해 REPLICA IDENTITY FULL 필수 (SPEC).

CREATE TABLE IF NOT EXISTS orders (
    id           BIGINT PRIMARY KEY,
    seller_id    BIGINT      NOT NULL,
    product_id   BIGINT      NOT NULL,
    amount       NUMERIC(12,2) NOT NULL,
    status       VARCHAR(20) NOT NULL DEFAULT 'CREATED',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE orders REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS inventory (
    id           BIGINT PRIMARY KEY,
    seller_id    BIGINT      NOT NULL,
    product_id   BIGINT      NOT NULL,
    quantity     INT         NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE inventory REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS settlements (
    id           BIGINT PRIMARY KEY,
    seller_id    BIGINT      NOT NULL,
    order_id     BIGINT      NOT NULL,
    settled_amount NUMERIC(12,2) NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE settlements REPLICA IDENTITY FULL;

-- Debezium incremental snapshot용 시그널 테이블 (Plan 09 재구축에서 사용)
CREATE TABLE IF NOT EXISTS debezium_signal (
    id    VARCHAR(64) PRIMARY KEY,
    type  VARCHAR(32) NOT NULL,
    data  VARCHAR(2048) NULL
);
ALTER TABLE debezium_signal REPLICA IDENTITY FULL;

-- Debezium 접속 계정에 논리 복제 권한 부여
ALTER ROLE app WITH REPLICATION;
