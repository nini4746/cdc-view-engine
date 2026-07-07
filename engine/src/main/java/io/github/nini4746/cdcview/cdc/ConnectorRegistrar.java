package io.github.nini4746.cdcview.cdc;

import io.github.nini4746.cdcview.config.BootstrapProperties;
import io.github.nini4746.cdcview.config.ConnectProperties;
import io.github.nini4746.cdcview.config.ViewRegistry;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * 부팅 시 Debezium PG 커넥터를 Kafka Connect REST API로 자동 등록한다(AC2-1).
 * PUT /connectors/{name}/config는 멱등이므로 재부팅해도 중복/에러가 없다.
 * 등록 전 REPLICA IDENTITY FULL을 선검사한다(AC2-3).
 */
@Component
@Order(10)
public class ConnectorRegistrar implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ConnectorRegistrar.class);

    private final ViewRegistry viewRegistry;
    private final ConnectorConfigFactory configFactory;
    private final ReplicaIdentityChecker replicaChecker;
    private final ConnectProperties connectProps;
    private final BootstrapProperties bootstrap;
    private final RestClient restClient;

    public ConnectorRegistrar(ViewRegistry viewRegistry,
                              ConnectorConfigFactory configFactory,
                              ReplicaIdentityChecker replicaChecker,
                              ConnectProperties connectProps,
                              BootstrapProperties bootstrap) {
        this.viewRegistry = viewRegistry;
        this.configFactory = configFactory;
        this.replicaChecker = replicaChecker;
        this.connectProps = connectProps;
        this.bootstrap = bootstrap;
        this.restClient = RestClient.builder().baseUrl(connectProps.getUrl()).build();
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!bootstrap.isConnectors()) {
            log.info("cdcview.bootstrap.connectors=false → 커넥터 자동 등록 생략");
            return;
        }
        Set<String> tables = viewRegistry.allSourceTables();
        if (tables.isEmpty()) {
            log.warn("소스 테이블이 없어 커넥터를 등록하지 않습니다");
            return;
        }
        // AC2-3: 등록 전 REPLICA IDENTITY FULL 선검사
        replicaChecker.verifyFull(tables);

        Map<String, Object> config = configFactory.build(tables);
        register(config);
        log.info("Debezium 커넥터 '{}' 등록 완료. 대상 테이블: {}", connectProps.getConnectorName(), tables);
    }

    private void register(Map<String, Object> config) {
        String name = connectProps.getConnectorName();
        restClient.put()
                .uri("/connectors/{name}/config", name)
                .header("Content-Type", "application/json")
                .body(config)
                .retrieve()
                .toBodilessEntity();
    }
}
