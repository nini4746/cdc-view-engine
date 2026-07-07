package io.github.nini4746.cdcview.verify;

import io.github.nini4746.cdcview.config.ViewDefinition;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * ViewDefinition → 기대값 SQL. 뷰별 하드코딩 없이 정의에서 파생한다.
 *
 * <p>토폴로지가 조인이 아닌 "소스별 그룹 집계 후 그룹 키로 병합" 이므로(조인 팬아웃 없음),
 * 기대값도 소스 테이블별로 GROUP BY 집계 SQL을 만들어 Java에서 그룹 키로 병합한다.
 * 비윈도우 뷰 대상. 윈도우 뷰는 MVP 범위에서 검증기가 지원하지 않는다.
 */
@Component
public class ExpectedViewSqlGenerator {

    /** 소스 테이블명 → 그 테이블의 그룹 집계 SQL. */
    public Map<String, String> sqlPerTable(ViewDefinition def) {
        if (def.isWindowed()) {
            throw new UnsupportedOperationException(
                    "윈도우 뷰 검증은 MVP 범위 밖입니다: " + def.getName());
        }
        boolean singleSource = def.getSources().size() == 1;
        Map<String, String> result = new LinkedHashMap<>();
        for (ViewDefinition.Source src : def.getSources()) {
            String table = src.getTable();
            String joinKey = Optional.ofNullable(src.getJoinKey()).orElse(def.getGroupBy());
            List<String> cols = new ArrayList<>();
            for (ViewDefinition.Aggregation agg : def.getAggregations()) {
                if (!singleSource && !table.equals(agg.getSource())) {
                    continue;
                }
                cols.add(aggColumn(agg));
            }
            StringBuilder sql = new StringBuilder("SELECT ")
                    .append(joinKey).append(" AS group_key");
            for (String c : cols) {
                sql.append(", ").append(c);
            }
            sql.append(" FROM ").append(table)
                    .append(" GROUP BY ").append(joinKey);
            result.put(table, sql.toString());
        }
        return result;
    }

    private String aggColumn(ViewDefinition.Aggregation agg) {
        String as = agg.getAs();
        return switch (agg.getFunction()) {
            case COUNT -> "COUNT(*) AS " + as;
            case SUM -> "COALESCE(SUM(" + agg.getColumn() + "),0) AS " + as;
            case MIN -> "MIN(" + agg.getColumn() + ") AS " + as;
            case MAX -> "MAX(" + agg.getColumn() + ") AS " + as;
            case AVG -> "AVG(" + agg.getColumn() + ") AS " + as;
        };
    }
}
