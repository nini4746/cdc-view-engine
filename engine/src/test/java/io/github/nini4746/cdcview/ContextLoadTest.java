package io.github.nini4746.cdcview;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * 컨텍스트 로드 스모크 테스트. Docker 없이 앱이 부팅 가능한지 확인한다(AC10-2의 단위 수준 대체).
 * 부팅 시 커넥터 등록/스트림 기동은 끄고, 뷰 디렉토리는 데모 YAML을 사용한다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "cdcview.bootstrap.connectors=false",
        "cdcview.bootstrap.streams=false",
        "cdcview.views.directory=../views"
})
class ContextLoadTest {

    @Autowired
    private HealthEndpoint healthEndpoint;

    @Test
    void contextLoadsAndHealthIsUp() {
        assertThat(healthEndpoint.health().getStatus()).isEqualTo(Status.UP);
    }
}
