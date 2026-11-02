package io.github.nini4746.cdcview.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.nini4746.cdcview.cdc.ChangeEvent;
import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewDefinitionException;
import io.github.nini4746.cdcview.config.ViewDefinitionLoader;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class CountDistinctTest {

    private final ViewDefinitionLoader loader = new ViewDefinitionLoader();
    private final ViewDefinition def = loader.loadFile(Path.of("../examples/ticket-fraud-views/card-daily.yaml"));

    private static Map<String, Object> row(String account) {
        Map<String, Object> row = new HashMap<>();
        row.put("card_fingerprint", "card-1");
        row.put("account_id", account);
        row.put("amount", 100L);
        return row;
    }

    private static ChangeEvent event(ChangeEvent.Op op, Map<String, Object> before, Map<String, Object> after) {
        return new ChangeEvent("payment", op, before, after, 1L, 1_000L);
    }

    private long distinct(AggregateState state) {
        return (long) state.results(def).get("distinct_accounts");
    }

    @Test
    void countsEachValueOnce() {
        AggregateState state = new AggregateState();
        state.apply(event(ChangeEvent.Op.CREATE, null, row("a")), def);
        state.apply(event(ChangeEvent.Op.CREATE, null, row("a")), def);
        state.apply(event(ChangeEvent.Op.CREATE, null, row("b")), def);
        assertThat(distinct(state)).isEqualTo(2);
    }

    // 같은 계정의 행이 하나 남아 있으면 삭제해도 계정 수는 줄지 않는다.
    @Test
    void deleteRemovesValueOnlyWithItsLastRow() {
        AggregateState state = new AggregateState();
        state.apply(event(ChangeEvent.Op.CREATE, null, row("a")), def);
        state.apply(event(ChangeEvent.Op.CREATE, null, row("a")), def);
        state.apply(event(ChangeEvent.Op.DELETE, row("a"), null), def);
        assertThat(distinct(state)).isEqualTo(1);
        state.apply(event(ChangeEvent.Op.DELETE, row("a"), null), def);
        assertThat(distinct(state)).isZero();
    }

    @Test
    void updateMovesRowBetweenValues() {
        AggregateState state = new AggregateState();
        state.apply(event(ChangeEvent.Op.CREATE, null, row("a")), def);
        state.apply(event(ChangeEvent.Op.UPDATE, row("a"), row("b")), def);
        assertThat(state.getDistincts().get("distinct_accounts")).containsOnlyKeys("b");
    }

    @Test
    void nullIsNotCounted() {
        AggregateState state = new AggregateState();
        state.apply(event(ChangeEvent.Op.CREATE, null, row(null)), def);
        assertThat(distinct(state)).isZero();
    }

    // state store 직렬화를 거쳐도 유지되어야 재시작 후 값이 맞다.
    @Test
    void survivesJsonRoundTrip() throws Exception {
        AggregateState state = new AggregateState();
        state.apply(event(ChangeEvent.Op.CREATE, null, row("a")), def);
        ObjectMapper mapper = new ObjectMapper();
        AggregateState copy = mapper.readValue(mapper.writeValueAsBytes(state), AggregateState.class);
        assertThat(distinct(copy)).isEqualTo(1);
    }

    @Test
    void featureFieldMustExist() {
        assertThatThrownBy(() -> loader.loadFile(resource("view-defs/feature-bad-field.yaml")))
                .isInstanceOf(ViewDefinitionException.class)
                .hasMessageContaining("features[0].field");
    }

    @Test
    void featureWindowMustMatchView() {
        assertThatThrownBy(() -> loader.loadFile(resource("view-defs/feature-window-mismatch.yaml")))
                .isInstanceOf(ViewDefinitionException.class)
                .hasMessageContaining("features[0].window");
    }

    private Path resource(String name) {
        try {
            return Path.of(Objects.requireNonNull(getClass().getClassLoader().getResource(name)).toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }
}
