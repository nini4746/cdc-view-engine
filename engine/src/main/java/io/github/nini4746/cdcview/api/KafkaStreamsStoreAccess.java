package io.github.nini4746.cdcview.api;

import io.github.nini4746.cdcview.topology.AggregateState;
import io.github.nini4746.cdcview.topology.StreamsManager;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.stereotype.Component;

/** 프로덕션 store 조회: 메인 KafkaStreams 인스턴스에서 read-only store를 얻는다. */
@Component
public class KafkaStreamsStoreAccess implements StoreAccess {

    private final StreamsManager streamsManager;

    public KafkaStreamsStoreAccess(StreamsManager streamsManager) {
        this.streamsManager = streamsManager;
    }

    @Override
    public boolean running() {
        return streamsManager.isRunning();
    }

    @Override
    public Optional<AggregateState> get(String storeName, String key) {
        KafkaStreams streams = requireStreams();
        try {
            ReadOnlyKeyValueStore<String, AggregateState> store = streams.store(
                    StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
            return Optional.ofNullable(store.get(key));
        } catch (InvalidStateStoreException e) {
            throw new StreamsNotReadyException("store 준비 안 됨: " + storeName, e);
        }
    }

    @Override
    public List<Map.Entry<Long, AggregateState>> windowFetch(
            String storeName, String key, long fromMs, long toMs) {
        KafkaStreams streams = requireStreams();
        try {
            ReadOnlyWindowStore<String, AggregateState> store = streams.store(
                    StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore()));
            List<Map.Entry<Long, AggregateState>> out = new ArrayList<>();
            try (WindowStoreIterator<AggregateState> it =
                         store.fetch(key, Instant.ofEpochMilli(fromMs), Instant.ofEpochMilli(toMs))) {
                while (it.hasNext()) {
                    var kv = it.next();
                    out.add(new AbstractMap.SimpleImmutableEntry<>(kv.key, kv.value));
                }
            }
            return out;
        } catch (InvalidStateStoreException e) {
            throw new StreamsNotReadyException("window store 준비 안 됨: " + storeName, e);
        }
    }

    @Override
    public List<Map.Entry<String, AggregateState>> scanAll(String storeName) {
        KafkaStreams streams = requireStreams();
        try {
            ReadOnlyKeyValueStore<String, AggregateState> store = streams.store(
                    StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
            List<Map.Entry<String, AggregateState>> out = new ArrayList<>();
            try (var it = store.all()) {
                while (it.hasNext()) {
                    var kv = it.next();
                    out.add(new AbstractMap.SimpleImmutableEntry<>(kv.key, kv.value));
                }
            }
            return out;
        } catch (InvalidStateStoreException e) {
            throw new StreamsNotReadyException("store 준비 안 됨: " + storeName, e);
        }
    }

    private KafkaStreams requireStreams() {
        KafkaStreams streams = streamsManager.streams();
        if (streams == null || !streamsManager.isRunning()) {
            throw new StreamsNotReadyException("Streams가 RUNNING 상태가 아닙니다");
        }
        return streams;
    }
}
