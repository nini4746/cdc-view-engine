package io.github.nini4746.cdcview.api;

import io.github.nini4746.cdcview.topology.AggregateState;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * state store 조회 추상화. 프로덕션은 KafkaStreams, 테스트는 페이크로 대체 가능하게 한다.
 */
public interface StoreAccess {

    /** 스트림이 조회 가능한 상태(RUNNING)인지. false면 503. */
    boolean running();

    /** 비윈도우 뷰: 키의 최신 집계 상태. */
    Optional<AggregateState> get(String storeName, String key);

    /**
     * 윈도우 뷰: [fromMs, toMs] 범위와 겹치는 윈도우들.
     * 각 엔트리 키는 윈도우 시작 시각(epoch ms).
     */
    List<Map.Entry<Long, AggregateState>> windowFetch(String storeName, String key, long fromMs, long toMs);
}
