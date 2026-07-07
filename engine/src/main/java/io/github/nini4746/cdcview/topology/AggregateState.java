package io.github.nini4746.cdcview.topology;

import io.github.nini4746.cdcview.cdc.ChangeEvent;
import io.github.nini4746.cdcview.config.ViewDefinition;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 한 그룹 키의 집계 누산 상태. state store에 저장되는 값.
 *
 * <p>증분 누산 방식: INSERT/READ는 after를 가산, UPDATE는 before 감산 + after 가산, DELETE는 before 감산.
 * sum/count/avg는 O(1) 증분, min/max는 값 멀티셋으로 감산 가능하게 유지한다.
 * freshness 메타(lastCommitTs/lastAppliedAt/lastLsn)를 함께 보관해 Plan 05 REST가 사용한다.
 *
 * <p>한계: 그룹 키가 바뀌는 UPDATE(예: 주문의 seller_id 변경)는 MVP 범위 밖이다(데모 도메인은 불변).
 */
public class AggregateState {

    private Map<String, Long> counters = new HashMap<>();      // COUNT: 필드 -> 행 수
    private Map<String, Double> sums = new HashMap<>();        // SUM: 필드 -> 합
    private Map<String, Double> avgSums = new HashMap<>();     // AVG: 필드 -> 합(분자)
    private Map<String, Long> avgCounts = new HashMap<>();     // AVG: 필드 -> 개수(분모)
    private Map<String, Map<String, Integer>> extremes = new HashMap<>(); // MIN/MAX: 필드 -> (값 -> 개수)

    private long lastCommitTs = -1;
    private long lastAppliedAt = -1;
    private long lastLsn = -1;

    /** 이벤트 1건을 누산에 반영한다. */
    public AggregateState apply(ChangeEvent ev, ViewDefinition def) {
        boolean singleSource = def.getSources().size() == 1;
        switch (ev.op()) {
            case CREATE, READ -> mutate(def, ev.table(), singleSource, ev.after(), +1);
            case DELETE -> mutate(def, ev.table(), singleSource, ev.before(), -1);
            case UPDATE -> {
                mutate(def, ev.table(), singleSource, ev.before(), -1);
                mutate(def, ev.table(), singleSource, ev.after(), +1);
            }
            default -> throw new IllegalStateException("unhandled op " + ev.op());
        }
        this.lastCommitTs = Math.max(this.lastCommitTs, ev.tsMs());
        this.lastLsn = ev.lsn();
        this.lastAppliedAt = System.currentTimeMillis();
        return this;
    }

    private void mutate(ViewDefinition def, String table, boolean singleSource,
                        Map<String, Object> image, int sign) {
        if (image == null) {
            return;
        }
        for (ViewDefinition.Aggregation agg : def.getAggregations()) {
            if (!appliesTo(agg, table, singleSource)) {
                continue;
            }
            String field = agg.getAs();
            switch (agg.getFunction()) {
                case COUNT -> counters.merge(field, (long) sign, Long::sum);
                case SUM -> {
                    Double v = numeric(image.get(agg.getColumn()));
                    if (v != null) {
                        sums.merge(field, sign * v, Double::sum);
                    }
                }
                case AVG -> {
                    Double v = numeric(image.get(agg.getColumn()));
                    if (v != null) {
                        avgSums.merge(field, sign * v, Double::sum);
                        avgCounts.merge(field, (long) sign, Long::sum);
                    }
                }
                case MIN, MAX -> {
                    Double v = numeric(image.get(agg.getColumn()));
                    if (v != null) {
                        multiset(field, v, sign);
                    }
                }
                default -> throw new IllegalStateException("unhandled function " + agg.getFunction());
            }
        }
    }

    private boolean appliesTo(ViewDefinition.Aggregation agg, String table, boolean singleSource) {
        if (singleSource) {
            return true;
        }
        return table.equals(agg.getSource());
    }

    private void multiset(String field, double value, int sign) {
        Map<String, Integer> ms = extremes.computeIfAbsent(field, k -> new HashMap<>());
        String key = Double.toString(value);
        int next = ms.getOrDefault(key, 0) + sign;
        if (next <= 0) {
            ms.remove(key);
        } else {
            ms.put(key, next);
        }
    }

    private static Double numeric(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Number n) {
            return n.doubleValue();
        }
        try {
            return Double.parseDouble(o.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** 뷰 정의 순서대로 계산된 결과 필드 맵. Plan 05 REST 응답에 쓰인다. */
    public Map<String, Object> results(ViewDefinition def) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (ViewDefinition.Aggregation agg : def.getAggregations()) {
            String field = agg.getAs();
            out.put(field, switch (agg.getFunction()) {
                case COUNT -> counters.getOrDefault(field, 0L);
                case SUM -> sums.getOrDefault(field, 0.0);
                case AVG -> {
                    long n = avgCounts.getOrDefault(field, 0L);
                    yield n == 0 ? null : avgSums.getOrDefault(field, 0.0) / n;
                }
                case MIN -> extreme(field, true);
                case MAX -> extreme(field, false);
            });
        }
        return out;
    }

    private Double extreme(String field, boolean min) {
        Map<String, Integer> ms = extremes.get(field);
        if (ms == null || ms.isEmpty()) {
            return null;
        }
        Double best = null;
        for (String k : ms.keySet()) {
            double v = Double.parseDouble(k);
            if (best == null || (min ? v < best : v > best)) {
                best = v;
            }
        }
        return best;
    }

    // ---- getters/setters (Jackson) ----
    public Map<String, Long> getCounters() {
        return counters;
    }

    public void setCounters(Map<String, Long> counters) {
        this.counters = counters;
    }

    public Map<String, Double> getSums() {
        return sums;
    }

    public void setSums(Map<String, Double> sums) {
        this.sums = sums;
    }

    public Map<String, Double> getAvgSums() {
        return avgSums;
    }

    public void setAvgSums(Map<String, Double> avgSums) {
        this.avgSums = avgSums;
    }

    public Map<String, Long> getAvgCounts() {
        return avgCounts;
    }

    public void setAvgCounts(Map<String, Long> avgCounts) {
        this.avgCounts = avgCounts;
    }

    public Map<String, Map<String, Integer>> getExtremes() {
        return extremes;
    }

    public void setExtremes(Map<String, Map<String, Integer>> extremes) {
        this.extremes = extremes;
    }

    public long getLastCommitTs() {
        return lastCommitTs;
    }

    public void setLastCommitTs(long lastCommitTs) {
        this.lastCommitTs = lastCommitTs;
    }

    public long getLastAppliedAt() {
        return lastAppliedAt;
    }

    public void setLastAppliedAt(long lastAppliedAt) {
        this.lastAppliedAt = lastAppliedAt;
    }

    public long getLastLsn() {
        return lastLsn;
    }

    public void setLastLsn(long lastLsn) {
        this.lastLsn = lastLsn;
    }
}
