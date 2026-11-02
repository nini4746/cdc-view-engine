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
    private List<Feature> features = List.of();

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

    /** 이 뷰가 피처 조회 API로 노출하는 피처 목록. 없으면 빈 목록. */
    public List<Feature> getFeatures() {
        return features;
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
        COUNT, COUNT_DISTINCT, SUM, MIN, MAX, AVG
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

    /** 피처 조회 주체. */
    public enum SubjectType {
        ACCOUNT, IP, CARD
    }

    /** 집계 필드를 피처 참조 키(subject.name.window)로 노출하는 선언. group_by 값이 주체 키다. */
    public static class Feature {
        private SubjectType subject;
        private String name;
        private String window;
        private String field;

        public SubjectType getSubject() {
            return subject;
        }

        public String getName() {
            return name;
        }

        /** 참조 키에 쓰이는 윈도우 표기(예: 10m). 뷰의 window.size와 같은 길이여야 한다. */
        public String getWindow() {
            return window;
        }

        /** 값을 읽을 집계 결과 필드(as). */
        public String getField() {
            return field;
        }

        /** 정규 참조 키. 예: account.payment_attempts.10m */
        public String referenceKey() {
            return referenceKey(subject, name, window);
        }

        public static String referenceKey(SubjectType subject, String name, String window) {
            return subject.name().toLowerCase(java.util.Locale.ROOT) + "." + name + "." + window;
        }
    }

    public enum WindowType {
        TUMBLING, SLIDING
    }
}
