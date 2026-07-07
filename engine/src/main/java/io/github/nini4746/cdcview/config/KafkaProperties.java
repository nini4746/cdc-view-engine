package io.github.nini4746.cdcview.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** cdcview.kafka.* - Kafka Streams 접속/식별 정보. */
@ConfigurationProperties(prefix = "cdcview.kafka")
public class KafkaProperties {

    private String bootstrapServers = "localhost:9092";
    private String applicationId = "cdc-view-engine";
    private String stateDir = "./data/kafka-streams";

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getStateDir() {
        return stateDir;
    }

    public void setStateDir(String stateDir) {
        this.stateDir = stateDir;
    }
}
