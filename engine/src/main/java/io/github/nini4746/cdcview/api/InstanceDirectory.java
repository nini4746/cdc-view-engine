package io.github.nini4746.cdcview.api;

import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.springframework.stereotype.Component;

/**
 * 조회 가능한 KafkaStreams 인스턴스 디렉토리(메인 + 재구축 섀도).
 * store 이름이 어느 인스턴스에 있든 찾아준다. Plan 09 스왑 후 새 버전 store는 섀도 인스턴스에 있다.
 */
@Component
public class InstanceDirectory {

    private final CopyOnWriteArrayList<KafkaStreams> instances = new CopyOnWriteArrayList<>();

    public void register(KafkaStreams streams) {
        instances.addIfAbsent(streams);
    }

    public void unregister(KafkaStreams streams) {
        instances.remove(streams);
    }

    public boolean anyRunning() {
        return instances.stream().anyMatch(s -> s.state().isRunningOrRebalancing());
    }

    /** 지정 store를 가진 실행 중 인스턴스에서 read-only store 핸들을 얻는다. */
    public <T> T store(String storeName, QueryableStoreType<T> type) {
        if (instances.isEmpty()) {
            throw new StreamsNotReadyException("실행 중인 Streams 인스턴스가 없습니다");
        }
        InvalidStateStoreException last = null;
        for (KafkaStreams s : instances) {
            if (!s.state().isRunningOrRebalancing()) {
                continue;
            }
            try {
                return s.store(StoreQueryParameters.fromNameAndType(storeName, type));
            } catch (InvalidStateStoreException e) {
                last = e; // 이 인스턴스에는 없음 → 다음 시도
            }
        }
        throw new StreamsNotReadyException("store를 찾을 수 없거나 준비 안 됨: " + storeName, last);
    }
}
