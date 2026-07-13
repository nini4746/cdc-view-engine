package io.github.nini4746.cdcview.rebuild;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 소스 Postgres(엔진이 이미 information_schema 조회/정합성 검증에 쓰는 {@code sourceJdbcTemplate})에
 * 재구축 상태를 영속화한다. 새 인프라를 들이지 않고 기존 DataSource를 재사용한다.
 *
 * <p>테이블은 첫 사용 시 지연 생성한다(앱 부팅 시 PG 연결을 강제하지 않기 위함 - DataSourceConfig가
 * initializationFailTimeout=-1로 부팅 시 PG 미가용을 허용하는 것과 동일한 전제).
 */
@Component
public class JdbcRebuildStateStore implements RebuildStateStore {

    private static final String CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS cdcview_rebuild_state (
                view_name    VARCHAR(255) PRIMARY KEY,
                status       VARCHAR(32) NOT NULL,
                from_version INT NOT NULL,
                to_version   INT NOT NULL,
                message      VARCHAR(2048),
                started_at   BIGINT NOT NULL,
                updated_at   BIGINT NOT NULL
            )
            """;

    private static final String UPSERT_SQL = """
            INSERT INTO cdcview_rebuild_state
                (view_name, status, from_version, to_version, message, started_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (view_name) DO UPDATE SET
                status = EXCLUDED.status,
                from_version = EXCLUDED.from_version,
                to_version = EXCLUDED.to_version,
                message = EXCLUDED.message,
                started_at = EXCLUDED.started_at,
                updated_at = EXCLUDED.updated_at
            """;

    private static final String FIND_SQL = """
            SELECT view_name, status, from_version, to_version, message, started_at, updated_at
            FROM cdcview_rebuild_state WHERE view_name = ?
            """;

    private final JdbcTemplate jdbc;
    private final AtomicBoolean tableEnsured = new AtomicBoolean(false);

    public JdbcRebuildStateStore(JdbcTemplate sourceJdbcTemplate) {
        this.jdbc = sourceJdbcTemplate;
    }

    private void ensureTable() {
        if (tableEnsured.compareAndSet(false, true)) {
            jdbc.execute(CREATE_TABLE_SQL);
        }
    }

    @Override
    public void save(RebuildState state) {
        ensureTable();
        jdbc.update(UPSERT_SQL,
                state.view(), state.status().name(), state.fromVersion(), state.toVersion(),
                state.message(), state.startedAt(), state.updatedAt());
    }

    @Override
    public Optional<RebuildState> find(String view) {
        ensureTable();
        List<RebuildState> rows = jdbc.query(FIND_SQL,
                (rs, rowNum) -> new RebuildState(
                        rs.getString("view_name"),
                        RebuildState.Status.valueOf(rs.getString("status")),
                        rs.getInt("from_version"),
                        rs.getInt("to_version"),
                        rs.getString("message"),
                        rs.getLong("started_at"),
                        rs.getLong("updated_at")),
                view);
        return rows.stream().findFirst();
    }
}
