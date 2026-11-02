package io.github.nini4746.cdcview.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.nini4746.cdcview.cdc.ChangeEvent;
import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewRegistry;
import io.github.nini4746.cdcview.config.ViewsProperties;
import io.github.nini4746.cdcview.topology.AggregateState;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** ticket-fraud-engine docs/cdc-view-engine-contract.md 계약을 검증한다. */
class FeatureQueryApiTest {

    private static final long MINUTE = 60_000L;
    private static final long NOW = 1_700_000_000_000L;

    private MockMvc mvc;
    private ViewQueryApiTest.FakeStoreAccess store;
    private ViewDefinition attempts;
    private ViewDefinition card;

    @BeforeEach
    void setUp() {
        ViewsProperties vp = new ViewsProperties();
        vp.setDirectory("../examples/ticket-fraud-views");
        ViewRegistry registry = new ViewRegistry(vp);
        registry.load();
        attempts = registry.find("account-payment-attempts").orElseThrow();
        card = registry.find("card-daily").orElseThrow();

        store = new ViewQueryApiTest.FakeStoreAccess();
        FeatureQueryService service = new FeatureQueryService(registry, store,
                Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC));
        mvc = MockMvcBuilders.standaloneSetup(new FeatureQueryController(service))
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
    }

    private static AggregateState payments(ViewDefinition def, int rows) {
        AggregateState state = new AggregateState();
        for (int i = 0; i < rows; i++) {
            state.apply(new ChangeEvent("payment", ChangeEvent.Op.CREATE, null,
                    Map.of("account_id", "acct-" + (i % 2), "card_fingerprint", "card-1", "amount", 100L),
                    i, NOW - 1_000), def);
        }
        return state;
    }

    private ResultActions query(String subject, String key, String features) throws Exception {
        return mvc.perform(post("/api/v1/features/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"subjectType\":\"" + subject + "\",\"subjectKey\":\"" + key
                        + "\",\"features\":" + features + "}"));
    }

    @Test
    void returnsValueUnderCanonicalKey() throws Exception {
        store.windows.put("view-account-payment-attempts-v1", Map.of("acct-1", List.of(
                new AbstractMap.SimpleImmutableEntry<>(NOW - MINUTE, payments(attempts, 3)))));

        query("ACCOUNT", "acct-1", "[{\"name\":\"payment_attempts\",\"window\":\"10m\"}]")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.values['account.payment_attempts.10m']").value(3.0))
                .andExpect(jsonPath("$.missing").isEmpty());
    }

    // 최근 10분을 가장 많이 덮는, 지금을 포함한 윈도우 중 가장 오래된 것을 읽는다.
    @Test
    void readsOldestWindowContainingNow() throws Exception {
        store.windows.put("view-account-payment-attempts-v1", Map.of("acct-1", List.of(
                new AbstractMap.SimpleImmutableEntry<>(NOW - 10 * MINUTE, payments(attempts, 9)), // 막 닫힘
                new AbstractMap.SimpleImmutableEntry<>(NOW - 9 * MINUTE, payments(attempts, 5)),
                new AbstractMap.SimpleImmutableEntry<>(NOW - MINUTE, payments(attempts, 1)))));

        query("ACCOUNT", "acct-1", "[{\"name\":\"payment_attempts\",\"window\":\"10m\"}]")
                .andExpect(jsonPath("$.values['account.payment_attempts.10m']").value(5.0));
    }

    @Test
    void noRowsInWindowIsZeroNotMissing() throws Exception {
        query("ACCOUNT", "new-acct", "[{\"name\":\"payment_attempts\",\"window\":\"10m\"}]")
                .andExpect(jsonPath("$.values['account.payment_attempts.10m']").value(0.0))
                .andExpect(jsonPath("$.missing").isEmpty());
    }

    @Test
    void servesSeveralFeaturesFromOneView() throws Exception {
        store.windows.put("view-card-daily-v1", Map.of("card-1", List.of(
                new AbstractMap.SimpleImmutableEntry<>(NOW - 1_000, payments(card, 3)))));

        query("CARD", "card-1", "[{\"name\":\"cumulative_amount\",\"window\":\"1d\"},"
                + "{\"name\":\"distinct_accounts\",\"window\":\"1d\"}]")
                .andExpect(jsonPath("$.values['card.cumulative_amount.1d']").value(300.0))
                .andExpect(jsonPath("$.values['card.distinct_accounts.1d']").value(2.0));
    }

    // 뷰가 없는 피처는 에러가 아니라 missing이어야 판정 엔진이 해당 룰만 건너뛴다.
    @Test
    void undeclaredFeatureIsMissing() throws Exception {
        query("IP", "1.2.3.4", "[{\"name\":\"distinct_accounts\",\"window\":\"1h\"}]")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.values").isEmpty())
                .andExpect(jsonPath("$.missing[0]").value("ip.distinct_accounts.1h"));
    }

    @Test
    void unknownSubjectTypeIs400() throws Exception {
        query("DEVICE", "d", "[]").andExpect(status().isBadRequest());
    }

    @Test
    void blankSubjectKeyIs400() throws Exception {
        query("ACCOUNT", " ", "[]").andExpect(status().isBadRequest());
    }
}
