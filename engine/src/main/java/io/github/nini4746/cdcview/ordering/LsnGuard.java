package io.github.nini4746.cdcview.ordering;

import io.github.nini4746.cdcview.cdc.ChangeEvent;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Component;

/**
 * 소스 행(PK) 단위로 최고 LSN을 유지해 stale/중복 이벤트를 폐기한다(last-write-wins).
 *
 * <p>같은 행에서 저장된 LSN 이하로 도착한 이벤트는 버린다. 동일 LSN(Debezium→Kafka at-least-once
 * 중복 재전달)도 "이하"라 자연 차단된다(Plan 07 멱등 근거). DELETE도 LSN을 갱신하므로 이후 낮은 LSN의
 * UPDATE로 행이 되살아나지 않는다(AC5-2). tombstone LSN은 store에 남으며, TTL 정리는 MVP 범위 밖이다.
 *
 * <p>키를 바꾸지 않고(FixedKey) 소스 토픽의 PK 파티셔닝을 그대로 활용한다. store 키는 table|pk.
 */
@Component
public class LsnGuard {

    private final OrderingMetrics metrics;

    public LsnGuard(OrderingMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * @param stream   소스 PK로 키잉된 스트림(재키잉 전)
     * @param viewName 카운터 태그용 뷰명
     * @param table    이 소스 테이블명(store 이름 유일화)
     */
    public KStream<String, ChangeEvent> apply(KStream<String, ChangeEvent> stream,
                                              String viewName, String table) {
        String storeName = "lsn-guard-" + viewName + "-" + table;
        return stream.processValues(new GuardSupplier(storeName, viewName, metrics));
    }

    private static final class GuardSupplier
            implements FixedKeyProcessorSupplier<String, ChangeEvent, ChangeEvent> {
        private final String storeName;
        private final String viewName;
        private final OrderingMetrics metrics;

        GuardSupplier(String storeName, String viewName, OrderingMetrics metrics) {
            this.storeName = storeName;
            this.viewName = viewName;
            this.metrics = metrics;
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            StoreBuilder<KeyValueStore<String, Long>> b = Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(storeName), Serdes.String(), Serdes.Long());
            return Set.of(b);
        }

        @Override
        public FixedKeyProcessor<String, ChangeEvent, ChangeEvent> get() {
            return new GuardProcessor(storeName, viewName, metrics);
        }
    }

    private static final class GuardProcessor
            implements FixedKeyProcessor<String, ChangeEvent, ChangeEvent> {
        private final String storeName;
        private final String viewName;
        private final OrderingMetrics metrics;
        private FixedKeyProcessorContext<String, ChangeEvent> ctx;
        private KeyValueStore<String, Long> store;

        GuardProcessor(String storeName, String viewName, OrderingMetrics metrics) {
            this.storeName = storeName;
            this.viewName = viewName;
            this.metrics = metrics;
        }

        @Override
        public void init(FixedKeyProcessorContext<String, ChangeEvent> context) {
            this.ctx = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(FixedKeyRecord<String, ChangeEvent> record) {
            ChangeEvent ev = record.value();
            String rowKey = ev.table() + "|" + record.key();
            Long seen = store.get(rowKey);
            if (seen != null && ev.lsn() <= seen) {
                // stale 또는 동일 LSN 중복 → 폐기(침묵 폐기 금지, 카운터로 관측)
                metrics.incStaleDropped(viewName);
                return;
            }
            store.put(rowKey, ev.lsn());
            ctx.forward(record);
        }
    }
}
