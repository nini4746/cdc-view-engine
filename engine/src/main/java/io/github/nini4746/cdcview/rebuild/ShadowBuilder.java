package io.github.nini4746.cdcview.rebuild;

import io.github.nini4746.cdcview.config.ViewDefinition;

/**
 * 섀도 뷰 빌드 추상화. 별도 KafkaStreams 인스턴스로 새 버전 store를 만들고 라이브를 따라잡게 한다.
 * 실 구현은 Kafka 기반(Docker 필요), 테스트는 즉시 완료 페이크로 대체한다.
 */
public interface ShadowBuilder {

    /** 새 버전 store(view-{name}-v{newVersion})를 스냅샷부터 빌드하는 섀도 스트림을 기동한다. */
    void startShadow(ViewDefinition def, int newVersion);

    /** 섀도가 라이브 오프셋을 따라잡았는지(lag 임계 이하). */
    boolean caughtUp(String view, int newVersion);

    /** 섀도를 서빙 대상으로 승격(스왑 후 조회가 새 버전 store를 찾을 수 있게). */
    void promote(String view, int newVersion);

    /** 섀도/임시 리소스 정리(실패 또는 구버전 교체 후). */
    void discard(String view, int version);
}
