package io.github.nini4746.cdcview.api;

import io.github.nini4746.cdcview.topology.AggregateState;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.stereotype.Component;

/** 프로덕션 store 조회: 인스턴스 디렉토리(메인 + 섀도)에서 read-only store를 얻는다. */
@Component
public class KafkaStreamsStoreAccess implements StoreAccess {

    private final InstanceDirectory directory;

    public KafkaStreamsStoreAccess(InstanceDirectory directory) {
        this.directory = directory;
    }

    @Override
    public boolean running() {
        return directory.anyRunning();
    }

    @Override
    public Optional<AggregateState> get(String storeName, String key) {
        ReadOnlyKeyValueStore<String, AggregateState> store =
                directory.store(storeName, QueryableStoreTypes.keyValueStore());
        return Optional.ofNullable(store.get(key));
    }

    @Override
    public List<Map.Entry<Long, AggregateState>> windowFetch(
            String storeName, String key, long fromMs, long toMs) {
        ReadOnlyWindowStore<String, AggregateState> store =
                directory.store(storeName, QueryableStoreTypes.windowStore());
        List<Map.Entry<Long, AggregateState>> out = new ArrayList<>();
        try (WindowStoreIterator<AggregateState> it =
                     store.fetch(key, Instant.ofEpochMilli(fromMs), Instant.ofEpochMilli(toMs))) {
            while (it.hasNext()) {
                var kv = it.next();
                out.add(new AbstractMap.SimpleImmutableEntry<>(kv.key, kv.value));
            }
        }
        return out;
    }

    @Override
    public List<Map.Entry<String, AggregateState>> scanAll(String storeName) {
        ReadOnlyKeyValueStore<String, AggregateState> store =
                directory.store(storeName, QueryableStoreTypes.keyValueStore());
        List<Map.Entry<String, AggregateState>> out = new ArrayList<>();
        try (var it = store.all()) {
            while (it.hasNext()) {
                var kv = it.next();
                out.add(new AbstractMap.SimpleImmutableEntry<>(kv.key, kv.value));
            }
        }
        return out;
    }
}
