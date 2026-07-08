package io.github.nini4746.cdcview.simulator;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 커머스 시뮬레이터. PG에 직접 INSERT/UPDATE/DELETE 하여 CDC 소스 변경을 만든다(Kafka에 직접 쓰지 않음).
 *
 * <p>환경변수: SIM_RATE(events/sec, 기본 100), SIM_SEED(결정성), SIM_DURATION(초, 0=무한),
 * SIM_SELLERS(기본 100). DB 접속은 POSTGRES_* 환경변수.
 */
public final class Simulator {

    private static final int PRODUCTS = 10;

    private final HikariDataSource ds;
    private final Random random;
    private final int rate;
    private final long durationMs;
    private final int sellers;

    private final AtomicLong orderSeq;
    private final AtomicLong settlementSeq;
    private final Deque<Long> recentOrders = new ArrayDeque<>();

    private long emitted;
    private long windowEmitted;
    private long lastLogAt;

    private Simulator() {
        this.rate = envInt("SIM_RATE", 100);
        this.sellers = envInt("SIM_SELLERS", 100);
        String seedEnv = env("SIM_SEED", null);
        long seed = seedEnv != null ? Long.parseLong(seedEnv) : System.nanoTime();
        this.random = new Random(seed);
        long durSec = envInt("SIM_DURATION", 0);
        this.durationMs = durSec <= 0 ? Long.MAX_VALUE : durSec * 1000L;

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(jdbcUrl());
        cfg.setUsername(env("POSTGRES_USER", "app"));
        cfg.setPassword(env("POSTGRES_PASSWORD", "app"));
        cfg.setMaximumPoolSize(4);
        this.ds = new HikariDataSource(cfg);

        this.orderSeq = new AtomicLong(maxId("orders") + 1);
        this.settlementSeq = new AtomicLong(maxId("settlements") + 1);
        System.out.printf("simulator start: rate=%d sellers=%d seed=%d duration=%s%n",
                rate, sellers, seed, durationMs == Long.MAX_VALUE ? "infinite" : durSec + "s");
    }

    public static void main(String[] args) throws Exception {
        Simulator sim = new Simulator();
        sim.seedInventory();
        sim.run();
    }

    /** 판매자별 재고 시드(id = seller_id). */
    private void seedInventory() {
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            for (int seller = 1; seller <= sellers; seller++) {
                st.addBatch("INSERT INTO inventory(id, seller_id, product_id, quantity) VALUES ("
                        + seller + "," + seller + ",1,1000) ON CONFLICT (id) DO NOTHING");
            }
            st.executeBatch();
        } catch (Exception e) {
            throw new RuntimeException("재고 시드 실패", e);
        }
    }

    private void run() throws Exception {
        long start = System.currentTimeMillis();
        lastLogAt = start;
        final long windowMs = 10;
        final long perWindow = Math.max(1, Math.round(rate * (windowMs / 1000.0)));

        while (System.currentTimeMillis() - start < durationMs) {
            long windowStart = System.nanoTime();
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                for (int i = 0; i < perWindow; i++) {
                    emitOne(c);
                }
                c.commit();
            }
            emitted += perWindow;
            windowEmitted += perWindow;
            maybeLog();

            long elapsedNs = System.nanoTime() - windowStart;
            long sleepMs = windowMs - elapsedNs / 1_000_000;
            if (sleepMs > 0) {
                Thread.sleep(sleepMs);
            }
        }
        System.out.printf("simulator done: total emitted=%d%n", emitted);
        ds.close();
    }

    private void emitOne(Connection c) throws Exception {
        int roll = random.nextInt(100);
        if (roll < 50) {
            insertOrder(c);
        } else if (roll < 65) {
            updateOrder(c);
        } else if (roll < 70) {
            deleteOrder(c);
        } else if (roll < 90) {
            updateInventory(c);
        } else {
            insertSettlement(c);
        }
    }

    private void insertOrder(Connection c) throws Exception {
        long id = orderSeq.getAndIncrement();
        int seller = 1 + random.nextInt(sellers);
        double amount = 10 + random.nextInt(490);
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO orders(id, seller_id, product_id, amount, status) VALUES (?,?,?,?,'CREATED')")) {
            ps.setLong(1, id);
            ps.setInt(2, seller);
            ps.setInt(3, 1 + random.nextInt(PRODUCTS));
            ps.setDouble(4, amount);
            ps.executeUpdate();
        }
        recentOrders.addLast(id);
        while (recentOrders.size() > 5000) {
            recentOrders.pollFirst();
        }
    }

    private void updateOrder(Connection c) throws Exception {
        Long id = randomRecentOrder();
        if (id == null) {
            insertOrder(c);
            return;
        }
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE orders SET status='PAID', amount=amount+1, updated_at=now() WHERE id=?")) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    private void deleteOrder(Connection c) throws Exception {
        Long id = randomRecentOrder();
        if (id == null) {
            return;
        }
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM orders WHERE id=?")) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    private void updateInventory(Connection c) throws Exception {
        int seller = 1 + random.nextInt(sellers);
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE inventory SET quantity=GREATEST(0, quantity-1), updated_at=now() WHERE id=?")) {
            ps.setInt(1, seller);
            ps.executeUpdate();
        }
    }

    private void insertSettlement(Connection c) throws Exception {
        Long orderId = randomRecentOrder();
        if (orderId == null) {
            insertOrder(c);
            return;
        }
        long id = settlementSeq.getAndIncrement();
        int seller = 1 + random.nextInt(sellers);
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO settlements(id, seller_id, order_id, settled_amount) VALUES (?,?,?,?)")) {
            ps.setLong(1, id);
            ps.setInt(2, seller);
            ps.setLong(3, orderId);
            ps.setDouble(4, 10 + random.nextInt(490));
            ps.executeUpdate();
        }
    }

    private Long randomRecentOrder() {
        if (recentOrders.isEmpty()) {
            return null;
        }
        // 결정적: 시드된 random으로 인덱스 선택(순회)
        int idx = random.nextInt(recentOrders.size());
        int i = 0;
        for (Long id : recentOrders) {
            if (i++ == idx) {
                return id;
            }
        }
        return recentOrders.peekLast();
    }

    private void maybeLog() {
        long now = System.currentTimeMillis();
        if (now - lastLogAt >= 5000) {
            double achieved = windowEmitted / ((now - lastLogAt) / 1000.0);
            System.out.printf("simulator rate: target=%d achieved=%.0f events/sec (total=%d)%n",
                    rate, achieved, emitted);
            windowEmitted = 0;
            lastLogAt = now;
        }
    }

    private long maxId(String table) {
        try (Connection c = ds.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COALESCE(MAX(id),0) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        } catch (Exception e) {
            return 0;
        }
    }

    private static String jdbcUrl() {
        String url = System.getenv("JDBC_URL");
        if (url != null) {
            return url;
        }
        return "jdbc:postgresql://" + env("POSTGRES_HOST", "localhost") + ":"
                + env("POSTGRES_PORT", "5432") + "/" + env("POSTGRES_DB", "demo");
    }

    /** 빈 문자열은 미설정으로 취급한다 (compose의 `${VAR:-}` 는 빈 값을 넘긴다). */
    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }

    private static int envInt(String key, int def) {
        String v = env(key, null);
        return v == null ? def : Integer.parseInt(v);
    }
}
