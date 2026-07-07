package io.github.nini4746.cdcview.topology;

import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.delete;
import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.insert;
import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.row;
import static io.github.nini4746.cdcview.testutil.DebeziumEnvelopes.update;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.nini4746.cdcview.cdc.CdcMetrics;
import io.github.nini4746.cdcview.config.ConnectProperties;
import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewDefinitionLoader;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TopologyMaterializeTest {

    private final ViewDefinitionLoader loader = new ViewDefinitionLoader();
    private final TopologyBuilder topologyBuilder =
            new TopologyBuilder(null, new ConnectProperties(), new CdcMetrics(new SimpleMeterRegistry()));
    private TopologyTestDriver driver;

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    private TopologyTestDriver build(ViewDefinition def, String store) {
        StreamsBuilder builder = new StreamsBuilder();
        topologyBuilder.buildView(builder, def, store);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-" + def.getName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(builder.build(), props);
        return driver;
    }

    private TestInputTopic<String, String> topic(String table) {
        return driver.createInputTopic("cdcview.public." + table,
                new StringSerializer(), new StringSerializer());
    }

    private Map<String, Object> dashboardResults(String store, String key, ViewDefinition def) {
        KeyValueStore<String, AggregateState> s = driver.getKeyValueStore(store);
        AggregateState st = s.get(key);
        return st == null ? null : st.results(def);
    }

    // AC3-1: 주문 INSERT 후 조인 뷰의 seller 집계가 갱신된다.
    @Test
    void insertUpdatesAggregate() {
        ViewDefinition def = loader.loadFile(Path.of("../views/seller-dashboard.yaml"));
        build(def, "view-seller-dashboard-v1");
        topic("orders").pipeInput("1",
                insert("orders", 100, 1_000, row("id", 1L, "seller_id", 10L, "amount", 100.0)));

        Map<String, Object> r = dashboardResults("view-seller-dashboard-v1", "10", def);
        assertThat(r).containsEntry("order_count", 1L);
        assertThat((double) r.get("total_order_amount")).isEqualTo(100.0);
    }

    // AC3-2: UPDATE 시 sum에서 이전 값 차감 + 새 값 가산 (100→150 이면 증가분 정확히 50).
    @Test
    void updateSubtractsOldAddsNew() {
        ViewDefinition def = loader.loadFile(Path.of("../views/seller-dashboard.yaml"));
        build(def, "view-seller-dashboard-v1");
        TestInputTopic<String, String> orders = topic("orders");
        orders.pipeInput("1", insert("orders", 100, 1_000, row("id", 1L, "seller_id", 10L, "amount", 100.0)));
        orders.pipeInput("1", update("orders", 200, 2_000,
                row("id", 1L, "seller_id", 10L, "amount", 100.0),
                row("id", 1L, "seller_id", 10L, "amount", 150.0)));

        Map<String, Object> r = dashboardResults("view-seller-dashboard-v1", "10", def);
        assertThat((double) r.get("total_order_amount")).isEqualTo(150.0);
        assertThat(r).containsEntry("order_count", 1L); // 여전히 주문 1건
    }

    // AC3-3: DELETE 시 해당 행 기여분이 제거된다.
    @Test
    void deleteRemovesContribution() {
        ViewDefinition def = loader.loadFile(Path.of("../views/seller-dashboard.yaml"));
        build(def, "view-seller-dashboard-v1");
        TestInputTopic<String, String> orders = topic("orders");
        orders.pipeInput("1", insert("orders", 100, 1_000, row("id", 1L, "seller_id", 10L, "amount", 100.0)));
        orders.pipeInput("1", delete("orders", 200, 2_000, row("id", 1L, "seller_id", 10L, "amount", 100.0)));

        Map<String, Object> r = dashboardResults("view-seller-dashboard-v1", "10", def);
        assertThat(r).containsEntry("order_count", 0L);
        assertThat((double) r.get("total_order_amount")).isEqualTo(0.0);
    }

    // min/max 감산 정확성: inventory 최소 재고가 행 삭제 시 재계산된다.
    @Test
    void minRecomputesOnDelete() {
        ViewDefinition def = loader.loadFile(Path.of("../views/seller-dashboard.yaml"));
        build(def, "view-seller-dashboard-v1");
        TestInputTopic<String, String> inv = topic("inventory");
        inv.pipeInput("1", insert("inventory", 100, 1_000, row("id", 1L, "seller_id", 10L, "quantity", 5L)));
        inv.pipeInput("2", insert("inventory", 110, 1_100, row("id", 2L, "seller_id", 10L, "quantity", 3L)));
        assertThat((double) dashboardResults("view-seller-dashboard-v1", "10", def).get("min_inventory"))
                .isEqualTo(3.0);

        inv.pipeInput("2", delete("inventory", 120, 1_200, row("id", 2L, "seller_id", 10L, "quantity", 3L)));
        assertThat((double) dashboardResults("view-seller-dashboard-v1", "10", def).get("min_inventory"))
                .isEqualTo(5.0);
    }

    // AC3-4: 윈도우 종료 시각과 정확히 같은 event time은 다음 윈도우에 속한다 ([start, end) 반개구간).
    @Test
    void windowBoundaryIsHalfOpen() {
        ViewDefinition def = loader.loadFile(resourcePath("view-defs/window-tumbling.yaml"));
        build(def, "view-win-v1");
        TestInputTopic<String, String> orders = topic("orders");
        // 윈도우 [0,1000). t=999는 이 윈도우, t=1000은 다음 윈도우 [1000,2000).
        orders.pipeInput("1", insert("orders", 1, 999, row("id", 1L, "seller_id", 10L, "amount", 10.0)));
        orders.pipeInput("2", insert("orders", 2, 1000, row("id", 2L, "seller_id", 10L, "amount", 10.0)));

        var store = driver.<String, AggregateState>getWindowStore("view-win-v1");
        Map<Long, Long> countsByWindowStart = new HashMap<>();
        try (WindowStoreIterator<AggregateState> it =
                     store.fetch("10", Instant.ofEpochMilli(0), Instant.ofEpochMilli(5000))) {
            while (it.hasNext()) {
                var kv = it.next();
                countsByWindowStart.put(kv.key, (Long) kv.value.results(def).get("cnt"));
            }
        }
        assertThat(countsByWindowStart).containsEntry(0L, 1L);      // t=999만
        assertThat(countsByWindowStart).containsEntry(1000L, 1L);   // t=1000은 다음 윈도우
    }

    private static Path resourcePath(String name) {
        try {
            return Path.of(TopologyMaterializeTest.class.getClassLoader().getResource(name).toURI());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
