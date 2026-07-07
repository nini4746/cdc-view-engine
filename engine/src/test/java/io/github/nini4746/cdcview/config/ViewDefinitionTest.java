package io.github.nini4746.cdcview.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ViewDefinitionTest {

    private final ViewDefinitionLoader loader = new ViewDefinitionLoader();

    private Path resource(String name) {
        try {
            return Path.of(getClass().getClassLoader().getResource(name).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    // AC1-1: 소스 2개 이상 + 조인 키 + 집계 + 윈도우 선언이 정상 로딩된다.
    @Test
    void loadsJoinedWindowedView() {
        ViewDefinition def = loader.loadFile(resource("view-defs/valid/joined.yaml"));
        assertThat(def.getName()).isEqualTo("valid-joined");
        assertThat(def.getSources()).hasSize(2);
        assertThat(def.getSources().get(0).getJoinKey()).isEqualTo("seller_id");
        assertThat(def.isWindowed()).isTrue();
        assertThat(def.getWindow().getSize()).isEqualTo(java.time.Duration.ofMinutes(5));
        assertThat(def.getWindow().getGrace()).isEqualTo(java.time.Duration.ofSeconds(30));
        assertThat(def.getAggregations()).hasSize(2);
    }

    // AC1-3: 저장소의 데모 뷰 2개가 정상 로딩되고 레지스트리에 등록된다.
    @Test
    void loadsBothDemoViews() {
        Map<String, ViewDefinition> defs = loader.loadDirectory(Path.of("../views"));
        assertThat(defs).containsKeys("seller-dashboard", "seller-sales-window");
        ViewDefinition dashboard = defs.get("seller-dashboard");
        assertThat(dashboard.tableNames()).containsExactly("orders", "inventory", "settlements");
        assertThat(dashboard.isWindowed()).isFalse();
        ViewDefinition window = defs.get("seller-sales-window");
        assertThat(window.isWindowed()).isTrue();
        assertThat(window.getWindow().getType()).isEqualTo(ViewDefinition.WindowType.SLIDING);
        assertThat(window.getWindow().getAdvance()).isEqualTo(java.time.Duration.ofMinutes(1));
    }

    // AC1-2: 필수 필드 누락 → 파일명 + 필드 경로 포함 예외.
    @Test
    void missingRequiredFieldFails() {
        ViewDefinitionException ex = catchThrowableOfType(
                () -> loader.loadFile(resource("view-defs/missing-required.yaml")),
                ViewDefinitionException.class);
        assertThat(ex).isNotNull();
        assertThat(ex.getFile()).isEqualTo("missing-required.yaml");
        assertThat(ex.getFieldPath()).isEqualTo("group_by");
        assertThat(ex.getMessage()).contains("missing-required.yaml", "group_by");
    }

    // AC1-2: 알 수 없는 필드(오타) → 필드 경로 포함 예외.
    @Test
    void unknownFieldFails() {
        ViewDefinitionException ex = catchThrowableOfType(
                () -> loader.loadFile(resource("view-defs/unknown-field.yaml")),
                ViewDefinitionException.class);
        assertThat(ex).isNotNull();
        assertThat(ex.getFile()).isEqualTo("unknown-field.yaml");
        assertThat(ex.getMessage()).contains("windw");
    }

    // sliding에 advance 누락 → 예외.
    @Test
    void slidingWithoutAdvanceFails() {
        assertThatThrownBy(() -> loader.loadFile(resource("view-defs/sliding-no-advance.yaml")))
                .isInstanceOf(ViewDefinitionException.class)
                .hasMessageContaining("window.advance");
    }

    // 뷰 이름 중복 → 예외.
    @Test
    void duplicateNameFails() {
        assertThatThrownBy(() -> loader.loadDirectory(resource("view-defs/dup")))
                .isInstanceOf(ViewDefinitionException.class)
                .hasMessageContaining("중복")
                .hasMessageContaining("dup-view");
    }
}
