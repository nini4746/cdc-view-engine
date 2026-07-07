package io.github.nini4746.cdcview.verify;

import io.github.nini4746.cdcview.config.ViewDefinition;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * REPEATABLE READ 스냅샷에서 소스별 그룹 집계 SQL을 실행해 기대값을 조립한다.
 * 한 트랜잭션 안에서 모든 소스 테이블을 조회해 시점 일관성을 확보한다.
 */
@Component
public class SqlExpectedViewProvider implements ExpectedViewProvider {

    private final JdbcTemplate jdbc;
    private final ExpectedViewSqlGenerator sqlGenerator;

    public SqlExpectedViewProvider(JdbcTemplate sourceJdbcTemplate,
                                   ExpectedViewSqlGenerator sqlGenerator) {
        this.jdbc = sourceJdbcTemplate;
        this.sqlGenerator = sqlGenerator;
    }

    @Override
    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public Map<String, Map<String, Object>> compute(ViewDefinition def) {
        Map<String, String> perTable = sqlGenerator.sqlPerTable(def);
        Map<String, Map<String, Object>> merged = new LinkedHashMap<>();
        for (String sql : perTable.values()) {
            List<Map<String, Object>> rows = jdbc.queryForList(sql);
            for (Map<String, Object> row : rows) {
                Object gk = row.get("group_key");
                if (gk == null) {
                    continue;
                }
                Map<String, Object> target =
                        merged.computeIfAbsent(String.valueOf(gk), k -> new LinkedHashMap<>());
                row.forEach((col, val) -> {
                    if (!"group_key".equals(col)) {
                        target.put(col, val);
                    }
                });
            }
        }
        return merged;
    }
}
