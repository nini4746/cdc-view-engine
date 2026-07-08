package io.github.nini4746.cdcview.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.nini4746.cdcview.config.BootstrapProperties;
import io.github.nini4746.cdcview.config.ConnectProperties;
import io.github.nini4746.cdcview.config.KafkaProperties;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Plan 12 (E1 수정): CDC 토픽 선생성의 이름 파생/멱등/실패 전파 검증 (AC12-4). */
class CdcTopicProvisionerTest {

    private final CdcTopicProvisioner provisioner = new CdcTopicProvisioner(
            null, new ConnectProperties(), new KafkaProperties(), new BootstrapProperties());

    private ListTopicsResult listResult(Set<String> existing) {
        ListTopicsResult r = mock(ListTopicsResult.class);
        when(r.names()).thenReturn(KafkaFuture.completedFuture(existing));
        return r;
    }

    private CreateTopicsResult createResult(Map<String, KafkaFuture<Void>> values) {
        CreateTopicsResult r = mock(CreateTopicsResult.class);
        when(r.values()).thenReturn(values);
        return r;
    }

    @Test
    void 토픽명은_topic_prefix와_public_스키마로_파생된다() {
        assertThat(provisioner.topicNames(List.of("orders", "inventory", "settlements")))
                .containsExactly("cdcview.public.orders", "cdcview.public.inventory",
                        "cdcview.public.settlements");
    }

    @Test
    @SuppressWarnings("unchecked")
    void 부재_토픽만_파티션1_RF1로_생성한다() {
        Admin admin = mock(Admin.class);
        ListTopicsResult listed = listResult(Set.of("cdcview.public.orders"));
        CreateTopicsResult created2 = createResult(
                Map.of("cdcview.public.inventory", KafkaFuture.completedFuture(null)));
        when(admin.listTopics()).thenReturn(listed);
        when(admin.createTopics(anyCollection(), any())).thenReturn(created2);

        provisioner.ensureTopics(admin, List.of("cdcview.public.orders", "cdcview.public.inventory"));

        ArgumentCaptor<java.util.Collection<NewTopic>> captor =
                ArgumentCaptor.forClass(java.util.Collection.class);
        verify(admin).createTopics(captor.capture(), any());
        assertThat(captor.getValue()).hasSize(1);
        NewTopic created = captor.getValue().iterator().next();
        assertThat(created.name()).isEqualTo("cdcview.public.inventory");
        assertThat(created.numPartitions()).isEqualTo(1);
        assertThat(created.replicationFactor()).isEqualTo((short) 1);
    }

    @Test
    void 전부_존재하면_생성하지_않는다() {
        Admin admin = mock(Admin.class);
        ListTopicsResult listed = listResult(Set.of("cdcview.public.orders", "cdcview.public.inventory"));
        when(admin.listTopics()).thenReturn(listed);

        provisioner.ensureTopics(admin, List.of("cdcview.public.orders", "cdcview.public.inventory"));

        verify(admin, never()).createTopics(anyCollection(), any());
    }

    @Test
    void 생성_경합의_TopicExists는_성공으로_취급한다() {
        Admin admin = mock(Admin.class);
        ListTopicsResult listed = listResult(Set.of());
        KafkaFutureImpl<Void> raced = new KafkaFutureImpl<>();
        raced.completeExceptionally(new TopicExistsException("already exists"));
        CreateTopicsResult racedResult = createResult(Map.of("cdcview.public.orders", raced));
        when(admin.listTopics()).thenReturn(listed);
        when(admin.createTopics(anyCollection(), any())).thenReturn(racedResult);

        assertThatCode(() -> provisioner.ensureTopics(admin, List.of("cdcview.public.orders")))
                .doesNotThrowAnyException();
    }

    @Test
    void 그_외_생성_실패는_부팅_중단_예외로_전파된다() {
        Admin admin = mock(Admin.class);
        ListTopicsResult listed = listResult(Set.of());
        KafkaFutureImpl<Void> failed = new KafkaFutureImpl<>();
        failed.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("broker down"));
        CreateTopicsResult failedResult = createResult(Map.of("cdcview.public.orders", failed));
        when(admin.listTopics()).thenReturn(listed);
        when(admin.createTopics(anyCollection(), any())).thenReturn(failedResult);

        assertThatThrownBy(() -> provisioner.ensureTopics(admin, List.of("cdcview.public.orders")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CDC 토픽 선생성 실패");
    }
}
