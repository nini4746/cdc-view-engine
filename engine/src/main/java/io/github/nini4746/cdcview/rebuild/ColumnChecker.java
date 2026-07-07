package io.github.nini4746.cdcview.rebuild;

import io.github.nini4746.cdcview.config.ViewDefinition;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 뷰가 참조하는 컬럼이 소스 스키마에 존재하는지 검증한다(AC8-5).
 * 참조 컬럼이 삭제된 상태로 재구축을 요청하면 컬럼명을 포함한 명시적 에러로 거부한다.
 */
@Component
public class ColumnChecker {

    private final JdbcTemplate jdbc;

    public ColumnChecker(JdbcTemplate sourceJdbcTemplate) {
        this.jdbc = sourceJdbcTemplate;
    }

    /** 뷰가 참조하는 테이블별 컬럼 집합(조인 키 + group_by + 집계 대상 컬럼). */
    public Map<String, Set<String>> referencedColumns(ViewDefinition def) {
        boolean single = def.getSources().size() == 1;
        Map<String, Set<String>> byTable = new LinkedHashMap<>();
        for (ViewDefinition.Source src : def.getSources()) {
            String keyCol = Optional.ofNullable(src.getJoinKey()).orElse(def.getGroupBy());
            byTable.computeIfAbsent(src.getTable(), t -> new LinkedHashSet<>()).add(keyCol);
        }
        for (ViewDefinition.Aggregation agg : def.getAggregations()) {
            if (agg.getColumn() == null) {
                continue; // count(*)
            }
            String table = single ? def.getSources().get(0).getTable() : agg.getSource();
            byTable.computeIfAbsent(table, t -> new LinkedHashSet<>()).add(agg.getColumn());
        }
        return byTable;
    }

    /** 참조 컬럼 중 존재하지 않는 것들을 "table.column" 형태로 반환(순수 함수, 테스트용). */
    public List<String> missing(ViewDefinition def, Map<String, Set<String>> existingByTable) {
        List<String> missing = new ArrayList<>();
        referencedColumns(def).forEach((table, cols) -> {
            Set<String> existing = existingByTable.getOrDefault(table, Set.of());
            for (String col : cols) {
                if (!existing.contains(col)) {
                    missing.add(table + "." + col);
                }
            }
        });
        return missing;
    }

    /** PG information_schema로 참조 컬럼 존재를 검증한다. 누락 시 예외. */
    public void verify(ViewDefinition def) {
        Map<String, Set<String>> existing = new LinkedHashMap<>();
        for (String table : referencedColumns(def).keySet()) {
            Set<String> cols = new LinkedHashSet<>(jdbc.queryForList(
                    "SELECT column_name FROM information_schema.columns "
                            + "WHERE table_schema='public' AND table_name=?",
                    String.class, table));
            existing.put(table, cols);
        }
        List<String> missing = missing(def, existing);
        if (!missing.isEmpty()) {
            throw new RebuildValidationException(
                    "재구축 거부: 뷰가 참조하는 컬럼이 소스에 없습니다: " + missing);
        }
    }
}
