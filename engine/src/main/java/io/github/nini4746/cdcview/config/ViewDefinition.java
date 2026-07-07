package io.github.nini4746.cdcview.config;

import java.time.Duration;
import java.util.List;

/**
 * YAML로 선언되는 뷰 정의. 필드 접근은 Jackson이 직접 수행한다(게터만 노출).
 * 알 수 없는 필드는 로더의 ObjectMapper 설정(FAIL_ON_UNKNOWN_PROPERTIES)으로 거부된다.
 */
public class ViewDefinition {

    private String name;
    private String description;
    private List<Source> sources;
    private String groupBy;
    private List<Aggregation> aggregations;
    private Window window;

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public List<Source> getSources() {
        return sources;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public List<Aggregation> getAggregations() {
        return aggregations;
    }

    public Window getWindow() {
        return window;
    }

    public boolean isWindowed() {
        return window != null;
    }

    /** 소스 테이블명 목록(중복 제거 전 원본 순서 유지). */
    public List<String> tableNames() {
        return sources.stream().map(Source::getTable).toList();
    }

    /** 조인/집계 대상 소스 정의. */
    public static class Source {
        private String table;
        private String joinKey;

        public String getTable() {
            return table;
        }

        /** 이 테이블에서 조인/그룹 값으로 쓰는 컬럼. 단일 소스 뷰에서는 생략 가능. */
        public String getJoinKey() {
            return joinKey;
        }
    }

    /** 집계 함수 화이트리스트. */
    public enum AggregateFunction {
        COUNT, SUM, MIN, MAX, AVG
    }

    /** 집계 항목: 함수 + 대상 컬럼 + 결과 필드명. */
    public static class Aggregation {
        private AggregateFunction function;
        private String column;
        private String as;
        private String source;

        public AggregateFunction getFunction() {
            return function;
        }

        /** 집계 대상 컬럼. count는 생략 가능. */
        public String getColumn() {
            return column;
        }

        /** 결과 필드명. */
        public String getAs() {
            return as;
        }

        /** 다중 소스 뷰에서 컬럼이 속한 테이블. */
        public String getSource() {
            return source;
        }
    }

    /** 윈도우 선언. tumbling 또는 sliding(hopping). */
    public static class Window {
        private WindowType type;
        private String size;
        private String advance;
        private String grace = "30s";

        public WindowType getType() {
            return type;
        }

        public Duration getSize() {
            return Durations.parse(size);
        }

        public Duration getAdvance() {
            return advance == null ? null : Durations.parse(advance);
        }

        public Duration getGrace() {
            return Durations.parse(grace);
        }

        String rawSize() {
            return size;
        }

        String rawAdvance() {
            return advance;
        }

        String rawGrace() {
            return grace;
        }
    }

    public enum WindowType {
        TUMBLING, SLIDING
    }
}
