package io.github.nini4746.cdcview.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** cdcview.bootstrap.* - 부팅 시 커넥터 등록/스트림 기동 여부. 단위 테스트에서는 끈다. */
@ConfigurationProperties(prefix = "cdcview.bootstrap")
public class BootstrapProperties {

    private boolean connectors = true;
    private boolean streams = true;

    public boolean isConnectors() {
        return connectors;
    }

    public void setConnectors(boolean connectors) {
        this.connectors = connectors;
    }

    public boolean isStreams() {
        return streams;
    }

    public void setStreams(boolean streams) {
        this.streams = streams;
    }
}
