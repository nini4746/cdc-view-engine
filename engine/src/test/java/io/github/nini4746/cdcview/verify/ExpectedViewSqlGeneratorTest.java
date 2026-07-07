package io.github.nini4746.cdcview.verify;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewDefinitionLoader;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExpectedViewSqlGeneratorTest {

    private final ExpectedViewSqlGenerator gen = new ExpectedViewSqlGenerator();
    private final ViewDefinitionLoader loader = new ViewDefinitionLoader();

    @Test
    void generatesPerTableAggregationSql() {
        ViewDefinition def = loader.loadFile(Path.of("../views/seller-dashboard.yaml"));
        Map<String, String> sql = gen.sqlPerTable(def);

        assertThat(sql).containsKeys("orders", "inventory", "settlements");
        assertThat(sql.get("orders"))
                .contains("SELECT seller_id AS group_key")
                .contains("COUNT(*) AS order_count")
                .contains("COALESCE(SUM(amount),0) AS total_order_amount")
                .contains("FROM orders")
                .contains("GROUP BY seller_id");
        assertThat(sql.get("inventory")).contains("MIN(quantity) AS min_inventory");
        assertThat(sql.get("settlements")).contains("COALESCE(SUM(settled_amount),0) AS total_settled_amount");
    }

    @Test
    void windowedViewIsUnsupported() {
        ViewDefinition win = loader.loadFile(Path.of("../views/seller-sales-window.yaml"));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> gen.sqlPerTable(win))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
