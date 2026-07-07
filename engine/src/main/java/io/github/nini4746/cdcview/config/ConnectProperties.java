package io.github.nini4746.cdcview.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** cdcview.connect.* - Kafka Connect(Debezium) REST와 커넥터/토픽 네이밍. */
@ConfigurationProperties(prefix = "cdcview.connect")
public class ConnectProperties {

    private String url = "http://localhost:8083";
    private String connectorName = "cdcview-postgres";
    /** Debezium topic.prefix. 토픽명은 {prefix}.{schema}.{table}. */
    private String topicPrefix = "cdcview";
    private String slotName = "cdcview_slot";
    private String publicationName = "cdcview_pub";

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public void setConnectorName(String connectorName) {
        this.connectorName = connectorName;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public String getSlotName() {
        return slotName;
    }

    public void setSlotName(String slotName) {
        this.slotName = slotName;
    }

    public String getPublicationName() {
        return publicationName;
    }

    public void setPublicationName(String publicationName) {
        this.publicationName = publicationName;
    }

    /** 테이블(public.orders)에 대응하는 CDC 토픽명. */
    public String topicFor(String schemaQualifiedTable) {
        return topicPrefix + "." + schemaQualifiedTable;
    }
}
