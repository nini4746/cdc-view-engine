package io.github.nini4746.cdcview.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** cdcview.kafka.* - Kafka Streams 접속/식별 정보. */
@ConfigurationProperties(prefix = "cdcview.kafka")
public class KafkaProperties {

    private String bootstrapServers = "localhost:9092";
    private String applicationId = "cdc-view-engine";
    private String stateDir = "./data/kafka-streams";
    /** Kafka Streams commit.interval.ms. EOS 처리량 대비 지연 트레이드오프 (T1 실험용). */
    private int commitIntervalMs = 100;
    /** Kafka Streams num.stream.threads. 처리량 병렬성 (T2). 파티션 수 이하에서만 유효. */
    private int streamThreads = 8;

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

    public int getCommitIntervalMs() {
        return commitIntervalMs;
    }

    public void setCommitIntervalMs(int commitIntervalMs) {
        this.commitIntervalMs = commitIntervalMs;
    }

    public int getStreamThreads() {
        return streamThreads;
    }

    public void setStreamThreads(int streamThreads) {
        this.streamThreads = streamThreads;
    }
}
