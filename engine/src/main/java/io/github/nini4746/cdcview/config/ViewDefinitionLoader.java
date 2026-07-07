package io.github.nini4746.cdcview.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * 지정 디렉토리의 *.yaml 뷰 정의를 파일 단위로 로딩/검증한다.
 * 알 수 없는 필드나 필수 필드 누락은 파일명 + 필드 경로를 담은 {@link ViewDefinitionException}으로 실패한다.
 */
public class ViewDefinitionLoader {

    private final ObjectMapper mapper;

    public ViewDefinitionLoader() {
        // snake_case YAML(join_key, group_by) → camelCase 필드 매핑. 알 수 없는 필드는 거부한다.
        this.mapper = new ObjectMapper(new YAMLFactory())
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);
    }

    /** 디렉토리의 모든 뷰 정의를 로딩·검증하고 이름→정의 맵을 반환한다. */
    public Map<String, ViewDefinition> loadDirectory(Path directory) {
        if (!Files.isDirectory(directory)) {
            throw new ViewDefinitionException(directory.toString(), null,
                    "뷰 정의 디렉토리가 존재하지 않습니다");
        }
        Map<String, ViewDefinition> byName = new LinkedHashMap<>();
        List<Path> files;
        try (Stream<Path> s = Files.list(directory)) {
            files = s.filter(p -> {
                String n = p.getFileName().toString().toLowerCase(Locale.ROOT);
                return n.endsWith(".yaml") || n.endsWith(".yml");
            }).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (Path file : files) {
            ViewDefinition def = loadFile(file);
            String fileName = file.getFileName().toString();
            if (byName.containsKey(def.getName())) {
                throw new ViewDefinitionException(fileName, "name",
                        "뷰 이름 중복: '" + def.getName() + "'");
            }
            byName.put(def.getName(), def);
        }
        return byName;
    }

    /** 단일 파일 로딩·검증. 테스트에서 직접 사용. */
    public ViewDefinition loadFile(Path file) {
        String fileName = file.getFileName().toString();
        ViewDefinition def;
        try {
            def = mapper.readValue(Files.readAllBytes(file), ViewDefinition.class);
        } catch (UnrecognizedPropertyException e) {
            throw new ViewDefinitionException(fileName, fieldPath(e), "알 수 없는 필드입니다", e);
        } catch (IOException e) {
            throw new ViewDefinitionException(fileName, null, "YAML 파싱 실패: " + e.getMessage(), e);
        }
        validate(fileName, def);
        return def;
    }

    private static String fieldPath(UnrecognizedPropertyException e) {
        List<String> parts = new ArrayList<>();
        e.getPath().forEach(ref -> {
            if (ref.getFieldName() != null) {
                parts.add(ref.getFieldName());
            } else if (ref.getIndex() >= 0) {
                parts.add("[" + ref.getIndex() + "]");
            }
        });
        parts.add(e.getPropertyName());
        return String.join(".", parts);
    }

    private void validate(String file, ViewDefinition def) {
        require(file, "name", def.getName());
        require(file, "sources", def.getSources());
        require(file, "group_by", def.getGroupBy());
        require(file, "aggregations", def.getAggregations());

        if (def.getSources().isEmpty()) {
            throw new ViewDefinitionException(file, "sources", "소스가 하나 이상 필요합니다");
        }
        boolean multiSource = def.getSources().size() >= 2;
        for (int i = 0; i < def.getSources().size(); i++) {
            ViewDefinition.Source src = def.getSources().get(i);
            String base = "sources[" + i + "]";
            require(file, base + ".table", src.getTable());
            if (multiSource && (src.getJoinKey() == null || src.getJoinKey().isBlank())) {
                throw new ViewDefinitionException(file, base + ".join_key",
                        "소스가 2개 이상이면 각 소스에 join_key가 필요합니다");
            }
        }

        if (def.getAggregations().isEmpty()) {
            throw new ViewDefinitionException(file, "aggregations", "집계가 하나 이상 필요합니다");
        }
        Set<String> resultNames = new HashSet<>();
        for (int i = 0; i < def.getAggregations().size(); i++) {
            ViewDefinition.Aggregation agg = def.getAggregations().get(i);
            String base = "aggregations[" + i + "]";
            require(file, base + ".function", agg.getFunction());
            require(file, base + ".as", agg.getAs());
            if (agg.getFunction() != ViewDefinition.AggregateFunction.COUNT
                    && (agg.getColumn() == null || agg.getColumn().isBlank())) {
                throw new ViewDefinitionException(file, base + ".column",
                        agg.getFunction() + " 집계에는 column이 필요합니다");
            }
            if (multiSource && (agg.getSource() == null || agg.getSource().isBlank())) {
                throw new ViewDefinitionException(file, base + ".source",
                        "다중 소스 뷰의 집계에는 source(테이블명)가 필요합니다");
            }
            if (agg.getSource() != null
                    && def.getSources().stream().noneMatch(s -> agg.getSource().equals(s.getTable()))) {
                throw new ViewDefinitionException(file, base + ".source",
                        "존재하지 않는 소스 테이블 참조: '" + agg.getSource() + "'");
            }
            if (!resultNames.add(agg.getAs())) {
                throw new ViewDefinitionException(file, base + ".as",
                        "집계 결과 필드명 중복: '" + agg.getAs() + "'");
            }
        }

        validateWindow(file, def.getWindow());
    }

    private void validateWindow(String file, ViewDefinition.Window window) {
        if (window == null) {
            return;
        }
        require(file, "window.type", window.getType());
        if (window.rawSize() == null) {
            throw new ViewDefinitionException(file, "window.size", "윈도우 size가 필요합니다");
        }
        parseDuration(file, "window.size", window.rawSize());
        parseDuration(file, "window.grace", window.rawGrace());

        if (window.getType() == ViewDefinition.WindowType.SLIDING) {
            if (window.rawAdvance() == null) {
                throw new ViewDefinitionException(file, "window.advance",
                        "sliding 윈도우에는 advance가 필요합니다");
            }
            parseDuration(file, "window.advance", window.rawAdvance());
        }
    }

    private Duration parseDuration(String file, String fieldPath, String raw) {
        try {
            return Durations.parse(raw);
        } catch (IllegalArgumentException e) {
            throw new ViewDefinitionException(file, fieldPath, e.getMessage());
        }
    }

    private void require(String file, String fieldPath, Object value) {
        if (value == null || (value instanceof String s && s.isBlank())) {
            throw new ViewDefinitionException(file, fieldPath, "필수 필드가 누락되었습니다");
        }
    }
}
