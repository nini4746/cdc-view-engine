package io.github.nini4746.cdcview.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.zaxxer.hikari.HikariDataSource;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * REPLICA IDENTITY 검사 SQL을 실제 PG 카탈로그로 검증한다(AC2-3).
 * Docker 미가용 환경에서는 전체가 skip 된다.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReplicaIdentityCheckerIT {

    private PostgreSQLContainer<?> pg;
    private HikariDataSource ds;
    private ReplicaIdentityChecker checker;

    @BeforeAll
    void setUp() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                "Docker 미가용 - REPLICA IDENTITY 통합 테스트 skip");
        pg = new PostgreSQLContainer<>("postgres:16");
        pg.start();
        ds = new HikariDataSource();
        ds.setJdbcUrl(pg.getJdbcUrl());
        ds.setUsername(pg.getUsername());
        ds.setPassword(pg.getPassword());
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        jdbc.execute("CREATE TABLE full_t (id BIGINT PRIMARY KEY, v INT)");
        jdbc.execute("ALTER TABLE full_t REPLICA IDENTITY FULL");
        jdbc.execute("CREATE TABLE default_t (id BIGINT PRIMARY KEY, v INT)");
        checker = new ReplicaIdentityChecker(jdbc);
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
    void passesWhenAllFull() {
        checker.verifyFull(List.of("full_t"));
    }

    @Test
    void failsWithTableNameWhenNotFull() {
        assertThatThrownBy(() -> checker.verifyFull(List.of("full_t", "default_t")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("default_t");
    }
}
