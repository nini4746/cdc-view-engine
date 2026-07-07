package io.github.nini4746.cdcview.rebuild;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewDefinitionLoader;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ColumnCheckerTest {

    private final ColumnChecker checker = new ColumnChecker(null);
    private final ViewDefinitionLoader loader = new ViewDefinitionLoader();

    private ViewDefinition dashboard() {
        return loader.loadFile(Path.of("../views/seller-dashboard.yaml"));
    }

    @Test
    void extractsReferencedColumnsPerTable() {
        Map<String, Set<String>> refs = checker.referencedColumns(dashboard());
        assertThat(refs.get("orders")).containsExactlyInAnyOrder("seller_id", "amount");
        assertThat(refs.get("inventory")).containsExactlyInAnyOrder("seller_id", "quantity");
        assertThat(refs.get("settlements")).containsExactlyInAnyOrder("seller_id", "settled_amount");
    }

    // AC8-5: 참조 컬럼이 삭제된 상태면 컬럼명을 포함해 검출한다.
    @Test
    void detectsMissingReferencedColumn() {
        Map<String, Set<String>> existing = Map.of(
                "orders", Set.of("seller_id", "amount"),
                "inventory", Set.of("seller_id", "quantity"),
                "settlements", Set.of("seller_id")); // settled_amount 삭제됨
        assertThat(checker.missing(dashboard(), existing))
                .containsExactly("settlements.settled_amount");
    }

    @Test
    void noMissingWhenAllPresent() {
        Map<String, Set<String>> existing = Map.of(
                "orders", Set.of("seller_id", "amount", "extra_new_col"),
                "inventory", Set.of("seller_id", "quantity"),
                "settlements", Set.of("seller_id", "settled_amount"));
        assertThat(checker.missing(dashboard(), existing)).isEmpty();
    }
}
