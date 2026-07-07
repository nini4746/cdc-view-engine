package io.github.nini4746.cdcview.config;

import jakarta.annotation.PostConstruct;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 부팅 시 뷰 정의 디렉토리를 로딩해 뷰 레지스트리를 구성한다.
 * 검증 실패 시 {@link ViewDefinitionException}을 던져 컨텍스트 초기화를 실패시킨다(exit != 0).
 *
 * <p>Plan 09의 무중단 스왑을 위해 뷰명 → 현재 서빙 store 버전 해석도 이 레지스트리에 집중한다.
 */
@Component
public class ViewRegistry {

    private static final Logger log = LoggerFactory.getLogger(ViewRegistry.class);

    private final ViewsProperties properties;
    private final ViewDefinitionLoader loader = new ViewDefinitionLoader();

    private volatile Map<String, ViewDefinition> definitions = Map.of();
    // 뷰명 → 현재 서빙 store 버전. 스왑(Plan 09) 시 volatile 교체.
    private final Map<String, Integer> servingVersion = new java.util.concurrent.ConcurrentHashMap<>();

    public ViewRegistry(ViewsProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void load() {
        Path dir = Path.of(properties.getDirectory());
        Map<String, ViewDefinition> loaded = loader.loadDirectory(dir);
        this.definitions = loaded;
        loaded.keySet().forEach(name -> servingVersion.putIfAbsent(name, 1));
        log.info("뷰 정의 {}개 로딩 완료: {}", loaded.size(), loaded.keySet());
    }

    /** 디렉토리를 다시 스캔해 뷰 정의를 갱신한다(Plan 09 재구축 시 편집된 YAML 반영). 서빙 버전 포인터는 보존. */
    public void reload() {
        load();
    }

    public Collection<ViewDefinition> all() {
        return definitions.values();
    }

    public Optional<ViewDefinition> find(String name) {
        return Optional.ofNullable(definitions.get(name));
    }

    public boolean contains(String name) {
        return definitions.containsKey(name);
    }

    /** 모든 뷰가 참조하는 소스 테이블의 합집합(커넥터 등록 대상). */
    public Set<String> allSourceTables() {
        Set<String> tables = new LinkedHashSet<>();
        definitions.values().forEach(def -> tables.addAll(def.tableNames()));
        return tables;
    }

    /** 현재 서빙 store 이름: view-{name}-v{N}. Plan 04/05/09에서 참조. */
    public String currentStoreName(String viewName) {
        return "view-" + viewName + "-v" + servingVersion.getOrDefault(viewName, 1);
    }

    public int currentVersion(String viewName) {
        return servingVersion.getOrDefault(viewName, 1);
    }

    /** Plan 09 원자적 스왑: 서빙 버전 포인터 교체. */
    public void swapVersion(String viewName, int newVersion) {
        servingVersion.put(viewName, newVersion);
        log.info("뷰 '{}' 서빙 버전 스왑 → v{}", viewName, newVersion);
    }
}
