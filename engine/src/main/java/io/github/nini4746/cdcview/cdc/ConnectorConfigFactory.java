package io.github.nini4746.cdcview.cdc;

import io.github.nini4746.cdcview.config.ConnectProperties;
import io.github.nini4746.cdcview.config.PostgresProperties;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/**
 * ViewRegistry의 소스 테이블 합집합으로 Debezium PG 커넥터 설정을 만든다.
 * value.converter.schemas.enable=false, decimal.handling.mode=double로 파서를 단순화한다.
 */
@Component
public class ConnectorConfigFactory {

    private final PostgresProperties pg;
    private final ConnectProperties connect;

    public ConnectorConfigFactory(PostgresProperties pg, ConnectProperties connect) {
        this.pg = pg;
        this.connect = connect;
    }

    /** @param tables 스키마 없는 테이블명 집합 */
    public Map<String, Object> build(Collection<String> tables) {
        String includeList = tables.stream()
                .map(t -> "public." + t)
                .collect(Collectors.joining(","));

        Map<String, Object> c = new LinkedHashMap<>();
        c.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        c.put("plugin.name", "pgoutput");
        c.put("database.hostname", pg.getCdcHost());
        c.put("database.port", String.valueOf(pg.getCdcPort()));
        c.put("database.user", pg.getUsername());
        c.put("database.password", pg.getPassword());
        c.put("database.dbname", pg.getDatabase());
        c.put("topic.prefix", connect.getTopicPrefix());
        c.put("table.include.list", includeList);
        c.put("snapshot.mode", "initial");
        // 지연 바닥 튜닝 (T1): poll.interval.ms 기본 500ms가 배치 누적 대기로 수백 ms 지연을
        // 유발. 낮은 값 + 작은 max.batch.size로 부분 배치를 빨리 방출한다.
        c.put("poll.interval.ms", String.valueOf(connect.getPollIntervalMs()));
        c.put("max.batch.size", String.valueOf(connect.getMaxBatchSize()));
        c.put("slot.name", connect.getSlotName());
        c.put("publication.name", connect.getPublicationName());
        c.put("publication.autocreate.mode", "filtered");
        // 금액(NUMERIC)을 double로 받아 집계를 단순화
        c.put("decimal.handling.mode", "double");
        // DELETE시 null tombstone을 만들지 않음(op=d 이벤트만)
        c.put("tombstones.on.delete", "false");
        c.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        c.put("key.converter.schemas.enable", "false");
        c.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        c.put("value.converter.schemas.enable", "false");
        // Plan 09 재구축용 incremental snapshot 시그널 테이블
        c.put("signal.data.collection", "public.debezium_signal");
        return c;
    }
}
