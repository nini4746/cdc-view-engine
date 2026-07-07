package io.github.nini4746.cdcview.cdc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 대상 테이블의 REPLICA IDENTITY가 FULL('f')인지 PG 카탈로그로 확인한다(AC2-3).
 * FULL이 아니면 테이블명을 포함한 예외로 커넥터 등록/부팅을 실패시킨다.
 */
@Component
public class ReplicaIdentityChecker {

    private final JdbcTemplate jdbc;

    public ReplicaIdentityChecker(JdbcTemplate sourceJdbcTemplate) {
        this.jdbc = sourceJdbcTemplate;
    }

    /**
     * @param tables 스키마 없는 테이블명 집합
     * @throws IllegalStateException FULL이 아닌 테이블이 있으면 테이블명 포함
     */
    public void verifyFull(Collection<String> tables) {
        if (tables.isEmpty()) {
            return;
        }
        Map<String, String> identities = fetchIdentities(tables);
        List<String> offenders = new ArrayList<>();
        for (String table : tables) {
            String id = identities.get(table);
            if (id == null) {
                offenders.add(table + "(테이블 없음)");
            } else if (!"f".equals(id)) {
                offenders.add(table + "(relreplident=" + id + ")");
            }
        }
        if (!offenders.isEmpty()) {
            throw new IllegalStateException(
                    "REPLICA IDENTITY FULL이 아닌 대상 테이블: " + offenders
                            + ". `ALTER TABLE <t> REPLICA IDENTITY FULL;` 적용 필요");
        }
    }

    private Map<String, String> fetchIdentities(Collection<String> tables) {
        Map<String, String> result = new java.util.HashMap<>();
        jdbc.query(
                "SELECT c.relname, c.relreplident FROM pg_class c "
                        + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                        + "WHERE n.nspname = 'public' AND c.relkind = 'r'",
                rs -> {
                    result.put(rs.getString("relname"), rs.getString("relreplident"));
                });
        return result;
    }
}
