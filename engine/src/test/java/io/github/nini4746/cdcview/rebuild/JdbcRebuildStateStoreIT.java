package io.github.nini4746.cdcview.rebuild;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.zaxxer.hikari.HikariDataSource;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * {@link JdbcRebuildStateStore}가 실제 PG 위에서 upsert/조회를 올바르게 수행하는지 검증한다.
 * Docker 미가용 환경에서는 skip 된다(ReplicaIdentityCheckerIT와 동일한 패턴).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JdbcRebuildStateStoreIT {

    private PostgreSQLContainer<?> pg;
    private HikariDataSource ds;
    private JdbcRebuildStateStore store;

    @BeforeAll
    void setUp() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                "Docker 미가용 - JdbcRebuildStateStore 통합 테스트 skip");
        pg = new PostgreSQLContainer<>("postgres:16");
        pg.start();
        ds = new HikariDataSource();
        ds.setJdbcUrl(pg.getJdbcUrl());
        ds.setUsername(pg.getUsername());
        ds.setPassword(pg.getPassword());
        store = new JdbcRebuildStateStore(new JdbcTemplate(ds));
    }

    @AfterAll
    void tearDown() {
        if (ds != null) {
            ds.close();
        }
        if (pg != null) {
            pg.stop();
        }
    }

    @Test
    void findReturnsEmptyWhenNoRecord() {
        assertThat(store.find("no-such-view")).isEmpty();
    }

    @Test
    void savePersistsAndFindRoundTrips() {
        RebuildState state = new RebuildState("seller-dashboard", RebuildState.Status.CATCHING_UP,
                1, 2, "라이브 따라잡기 대기", 1000L, 2000L);
        store.save(state);

        Optional<RebuildState> found = store.find("seller-dashboard");
        assertThat(found).contains(state);
    }

    @Test
    void saveUpsertsLatestStateForSameView() {
        store.save(new RebuildState("orders-summary", RebuildState.Status.VALIDATING,
                1, 2, "참조 컬럼 검증 중", 1000L, 1000L));
        RebuildState finalState = new RebuildState("orders-summary", RebuildState.Status.SWAPPED,
                1, 2, "v2 스왑 완료", 1000L, 5000L);
        store.save(finalState);

        Optional<RebuildState> found = store.find("orders-summary");
        assertThat(found).contains(finalState);
    }
}
