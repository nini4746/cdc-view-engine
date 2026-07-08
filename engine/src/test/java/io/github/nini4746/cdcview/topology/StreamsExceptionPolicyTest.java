package io.github.nini4746.cdcview.topology;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.junit.jupiter.api.Test;

/** Plan 12 (E1 수정): Streams 예외 처리 방침 - 소스 토픽 부재만 재시도, 그 외는 종료 (AC12-4). */
class StreamsExceptionPolicyTest {

    @Test
    void 소스_토픽_부재는_스레드_교체로_재시도한다() {
        assertThat(StreamsManager.responseFor(new MissingSourceTopicException("missing")))
                .isEqualTo(StreamThreadExceptionResponse.REPLACE_THREAD);
    }

    @Test
    void 래핑된_소스_토픽_부재도_cause_체인에서_인식한다() {
        StreamsException wrapped =
                new StreamsException("rebalance failed", new MissingSourceTopicException("missing"));
        assertThat(StreamsManager.responseFor(wrapped))
                .isEqualTo(StreamThreadExceptionResponse.REPLACE_THREAD);
    }

    @Test
    void 그_외_예외는_기존_기본_동작대로_클라이언트를_종료한다() {
        assertThat(StreamsManager.responseFor(new RuntimeException("processing bug")))
                .isEqualTo(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        assertThat(StreamsManager.responseFor(new StreamsException("io", new RuntimeException("x"))))
                .isEqualTo(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
    }
}
