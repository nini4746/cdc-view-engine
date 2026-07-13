package io.github.nini4746.cdcview.rebuild;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.nini4746.cdcview.config.ViewDefinition;
import io.github.nini4746.cdcview.config.ViewRegistry;
import io.github.nini4746.cdcview.config.ViewsProperties;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RebuildServiceTest {

    private ViewRegistry registry;
    private FakeRebuildStateStore store;

    @BeforeEach
    void setUp() {
        ViewsProperties vp = new ViewsProperties();
        vp.setDirectory("../views");
        registry = new ViewRegistry(vp);
        registry.load();
        store = new FakeRebuildStateStore();
    }

    private RebuildService service(ShadowBuilder shadow, ColumnChecker checker) {
        RebuildService s = new RebuildService(registry, checker, shadow, store);
        s.setPollParams(1, 1000);
        return s;
    }

    private ColumnChecker noopChecker() {
        return new ColumnChecker(null) {
            @Override
            public void verify(ViewDefinition def) {
                // no-op (PG 접근 없이 통과)
            }
        };
    }

    // AC8-2: rebuild → 섀도 빌드 → 원자적 스왑. 완료 후 서빙 버전이 v2로 교체된다.
    @Test
    void rebuildSwapsToNewVersion() {
        RecordingShadow shadow = new RecordingShadow(0);
        RebuildService svc = service(shadow, noopChecker());
        assertThat(registry.currentStoreName("seller-dashboard")).isEqualTo("view-seller-dashboard-v1");

        svc.rebuild("seller-dashboard");
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> svc.status("seller-dashboard").status() == RebuildState.Status.SWAPPED);

        assertThat(registry.currentStoreName("seller-dashboard")).isEqualTo("view-seller-dashboard-v2");
        assertThat(shadow.started).isTrue();
        assertThat(shadow.promoted).isTrue();
        // 구버전 정리 호출됨
        assertThat(shadow.discarded).contains("seller-dashboard-v1");
    }

    // AC8-3(부분): 스왑은 단일 참조 교체. 따라잡기 전에는 버전이 유지되고, 스왑 시 한 번에 바뀐다.
    @Test
    void versionStaysUntilCaughtUp() {
        // 느린 폴링으로 따라잡기 구간을 관측 가능하게 한다(50ms x 10회 ≈ 0.5s).
        RecordingShadow shadow = new RecordingShadow(10);
        RebuildService svc = new RebuildService(registry, noopChecker(), shadow, store);
        svc.setPollParams(50, 1000);

        svc.rebuild("seller-dashboard");
        // 스왑 전에는 여전히 v1 (부분 스왑 없음)
        assertThat(registry.currentVersion("seller-dashboard")).isEqualTo(1);

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> svc.status("seller-dashboard").status() == RebuildState.Status.SWAPPED);
        assertThat(registry.currentVersion("seller-dashboard")).isEqualTo(2);
    }

    // AC8-5: 참조 컬럼이 없으면 400(예외)로 거부, 서빙 버전 불변.
    @Test
    void missingColumnRejectsRebuild() {
        ColumnChecker failing = new ColumnChecker(null) {
            @Override
            public void verify(ViewDefinition def) {
                throw new RebuildValidationException("재구축 거부: settlements.settled_amount 없음");
            }
        };
        RebuildService svc = service(new RecordingShadow(0), failing);

        assertThatThrownBy(() -> svc.rebuild("seller-dashboard"))
                .isInstanceOf(RebuildValidationException.class)
                .hasMessageContaining("settled_amount");
        assertThat(registry.currentVersion("seller-dashboard")).isEqualTo(1);
    }

    // 진행 중 재요청은 거부(409).
    @Test
    void concurrentRebuildRejected() {
        RecordingShadow shadow = new RecordingShadow(1000); // 오래 걸리게
        RebuildService svc = service(shadow, noopChecker());
        svc.rebuild("seller-dashboard");
        assertThatThrownBy(() -> svc.rebuild("seller-dashboard"))
                .isInstanceOf(IllegalStateException.class);
    }

    // 크래시 감지: 진행 중이던 재구축을 새 서비스 인스턴스(재시작 시뮬레이션)가 발견하면 ABORTED로 전환한다.
    @Test
    void crashMidRebuildIsDetectedAsAbortedOnRestart() {
        RecordingShadow shadow = new RecordingShadow(Integer.MAX_VALUE); // 영원히 안 따라잡음(크래시 시뮬레이션용)
        RebuildService crashed = new RebuildService(registry, noopChecker(), shadow, store);
        crashed.setPollParams(5, 1_000_000);

        crashed.rebuild("seller-dashboard");
        Awaitility.await().atMost(Duration.ofSeconds(2))
                .until(() -> crashed.status("seller-dashboard").status() == RebuildState.Status.CATCHING_UP);

        // "재시작": 같은 store 위에 새 RebuildService 인스턴스(인메모리 맵은 비어 있음)
        RebuildService restarted = new RebuildService(registry, noopChecker(), new RecordingShadow(0), store);

        RebuildState afterRestart = restarted.status("seller-dashboard");
        assertThat(afterRestart.status()).isEqualTo(RebuildState.Status.ABORTED);
        assertThat(afterRestart.isTerminal()).isTrue();
        assertThat(afterRestart.message()).contains("재시작").contains("CATCHING_UP");
        // 이전 버전이 계속 서빙된다(스왑 안 됨)
        assertThat(registry.currentVersion("seller-dashboard")).isEqualTo(1);
        // store에도 ABORTED로 영속화되어 다음에 다시 봐도 같은 결론이 나온다
        assertThat(store.find("seller-dashboard")).get()
                .extracting(RebuildState::status).isEqualTo(RebuildState.Status.ABORTED);

        // ABORTED는 종결 상태이므로 재요청이 예외 없이 접수되고, 처음부터 다시 정상 완료된다
        restarted.setPollParams(5, 1000);
        RebuildState retried = restarted.rebuild("seller-dashboard");
        assertThat(retried.status()).isNotEqualTo(RebuildState.Status.ABORTED);
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> restarted.status("seller-dashboard").status() == RebuildState.Status.SWAPPED);
        assertThat(registry.currentVersion("seller-dashboard")).isEqualTo(2);
    }

    // 정상 완료 시에는 크래시로 오인되지 않고 SWAPPED 종결 레코드가 store에 남는다.
    @Test
    void normalCompletionPersistsSwappedRecordNotMisreadAsCrash() {
        RebuildService svc = service(new RecordingShadow(0), noopChecker());
        svc.rebuild("seller-dashboard");
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> svc.status("seller-dashboard").status() == RebuildState.Status.SWAPPED);

        assertThat(store.find("seller-dashboard")).get()
                .extracting(RebuildState::status).isEqualTo(RebuildState.Status.SWAPPED);

        // "재시작" 이후에도 정상 완료 기록은 그대로 SWAPPED로 보인다(ABORTED로 잘못 바뀌지 않음)
        RebuildService restarted = new RebuildService(registry, noopChecker(), new RecordingShadow(0), store);
        assertThat(restarted.status("seller-dashboard").status()).isEqualTo(RebuildState.Status.SWAPPED);
    }

    /** 인메모리 페이크 store. 여러 RebuildService 인스턴스가 같은 store를 공유해 "재시작"을 시뮬레이션한다. */
    static class FakeRebuildStateStore implements RebuildStateStore {
        private final ConcurrentHashMap<String, RebuildState> data = new ConcurrentHashMap<>();

        @Override
        public void save(RebuildState state) {
            data.put(state.view(), state);
        }

        @Override
        public Optional<RebuildState> find(String view) {
            return Optional.ofNullable(data.get(view));
        }
    }

    /** 호출을 기록하는 페이크 섀도 빌더. caughtUpAfter 회 이후 true. */
    static class RecordingShadow implements ShadowBuilder {
        private final int caughtUpAfter;
        private final AtomicInteger polls = new AtomicInteger();
        volatile boolean started;
        volatile boolean promoted;
        final java.util.List<String> discarded = new java.util.concurrent.CopyOnWriteArrayList<>();

        RecordingShadow(int caughtUpAfter) {
            this.caughtUpAfter = caughtUpAfter;
        }

        @Override
        public void startShadow(ViewDefinition def, int newVersion) {
            started = true;
        }

        @Override
        public boolean caughtUp(String view, int newVersion) {
            return polls.incrementAndGet() > caughtUpAfter;
        }

        @Override
        public void promote(String view, int newVersion) {
            promoted = true;
        }

        @Override
        public void discard(String view, int version) {
            discarded.add(view + "-v" + version);
        }
    }
}
