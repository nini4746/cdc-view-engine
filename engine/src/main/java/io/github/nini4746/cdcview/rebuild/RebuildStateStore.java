package io.github.nini4746.cdcview.rebuild;

import java.util.Optional;

/**
 * 재구축 생명주기 상태의 영속 저장소.
 *
 * <p>{@link RebuildService}의 인메모리 상태 맵을 미러링해 프로세스 재시작 후에도 크래시 시점에
 * 진행 중이던 재구축을 감지할 수 있게 한다(감시용 audit 대응). 뷰당 최신 레코드 하나만 유지한다.
 */
public interface RebuildStateStore {

    /** 뷰의 최신 재구축 상태를 upsert 한다. */
    void save(RebuildState state);

    /** 뷰의 마지막으로 기록된 재구축 상태(있다면). */
    Optional<RebuildState> find(String view);
}
