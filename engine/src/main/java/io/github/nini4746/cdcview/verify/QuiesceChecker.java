package io.github.nini4746.cdcview.verify;

/** 스트림 정지 상태(consumer lag 0) 판정. */
public interface QuiesceChecker {
    /** CDC consumer group의 총 lag. 0이면 quiesced로 본다. */
    long totalLag();

    default boolean isQuiesced() {
        return totalLag() == 0;
    }
}
