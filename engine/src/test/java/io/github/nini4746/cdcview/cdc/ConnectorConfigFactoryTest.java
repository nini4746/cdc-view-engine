package io.github.nini4746.cdcview.cdc;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.nini4746.cdcview.config.ConnectProperties;
import io.github.nini4746.cdcview.config.PostgresProperties;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConnectorConfigFactoryTest {

    private final ConnectorConfigFactory factory =
            new ConnectorConfigFactory(new PostgresProperties(), new ConnectProperties());

    @Test
    void buildsConnectorConfig() {
        Map<String, Object> c = factory.build(List.of("orders", "inventory", "settlements"));

        assertThat(c).containsEntry("connector.class",
                "io.debezium.connector.postgresql.PostgresConnector");
        assertThat(c).containsEntry("plugin.name", "pgoutput");
        assertThat(c.get("table.include.list"))
                .isEqualTo("public.orders,public.inventory,public.settlements");
        // 파서 단순화를 위한 핵심 설정
        assertThat(c).containsEntry("value.converter.schemas.enable", "false");
        assertThat(c).containsEntry("decimal.handling.mode", "double");
        assertThat(c).containsEntry("tombstones.on.delete", "false");
        assertThat(c).containsEntry("snapshot.mode", "initial");
    }
}
