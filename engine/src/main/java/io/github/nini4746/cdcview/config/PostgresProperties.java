package io.github.nini4746.cdcview.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** cdcview.postgres.* - 소스 PG 접속 정보. 스냅샷/검증기/커넥터 등록에서 사용. */
@ConfigurationProperties(prefix = "cdcview.postgres")
public class PostgresProperties {

    private String host = "localhost";
    private int port = 5432;
    private String database = "demo";
    private String username = "app";
    private String password = "app";
    /** Debezium이 접속할 호스트(컨테이너 네트워크 내부명일 수 있음). */
    private String cdcHost = "localhost";
    private int cdcPort = 5432;

    public String jdbcUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + database;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getCdcHost() {
        return cdcHost;
    }

    public void setCdcHost(String cdcHost) {
        this.cdcHost = cdcHost;
    }

    public int getCdcPort() {
        return cdcPort;
    }

    public void setCdcPort(int cdcPort) {
        this.cdcPort = cdcPort;
    }
}
