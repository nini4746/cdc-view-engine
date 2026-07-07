package io.github.nini4746.cdcview.cdc;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class ChangeEventParserTest {

    private final ChangeEventParser parser = new ChangeEventParser();

    private ChangeEvent parse(String json) {
        return parser.parse(json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void parsesInsert() {
        ChangeEvent e = parse("""
                {"before":null,
                 "after":{"id":1,"seller_id":10,"amount":100.0,"status":"CREATED"},
                 "source":{"table":"orders","lsn":1000,"ts_ms":1699999999000},
                 "op":"c","ts_ms":1700000000000}
                """);
        assertThat(e.op()).isEqualTo(ChangeEvent.Op.CREATE);
        assertThat(e.table()).isEqualTo("orders");
        assertThat(e.lsn()).isEqualTo(1000L);
        assertThat(e.tsMs()).isEqualTo(1699999999000L);
        assertThat(e.before()).isNull();
        assertThat(e.after()).containsEntry("amount", 100.0).containsEntry("seller_id", 10L);
    }

    @Test
    void parsesUpdateWithBeforeImage() {
        ChangeEvent e = parse("""
                {"before":{"id":1,"seller_id":10,"amount":100.0},
                 "after":{"id":1,"seller_id":10,"amount":150.0},
                 "source":{"table":"orders","lsn":2000,"ts_ms":1699999999500},
                 "op":"u","ts_ms":1700000000500}
                """);
        assertThat(e.op()).isEqualTo(ChangeEvent.Op.UPDATE);
        assertThat(e.before()).containsEntry("amount", 100.0);
        assertThat(e.after()).containsEntry("amount", 150.0);
    }

    @Test
    void parsesDeleteWithBeforeAndNullAfter() {
        ChangeEvent e = parse("""
                {"before":{"id":1,"seller_id":10,"amount":150.0},
                 "after":null,
                 "source":{"table":"orders","lsn":3000,"ts_ms":1699999999900},
                 "op":"d","ts_ms":1700000000900}
                """);
        assertThat(e.op()).isEqualTo(ChangeEvent.Op.DELETE);
        assertThat(e.isDelete()).isTrue();
        assertThat(e.after()).isNull();
        assertThat(e.effectiveImage()).containsEntry("amount", 150.0);
    }

    @Test
    void parsesSnapshotRead() {
        ChangeEvent e = parse("""
                {"before":null,
                 "after":{"id":2,"seller_id":11,"amount":42.0},
                 "source":{"table":"orders","lsn":500,"ts_ms":1699999998000},
                 "op":"r","ts_ms":1700000000000}
                """);
        assertThat(e.op()).isEqualTo(ChangeEvent.Op.READ);
    }

    @Test
    void unwrapsSchemaWrapperWhenPresent() {
        ChangeEvent e = parse("""
                {"schema":{"type":"struct"},
                 "payload":{"before":null,"after":{"id":9},
                   "source":{"table":"inventory","lsn":77,"ts_ms":1},"op":"c","ts_ms":2}}
                """);
        assertThat(e.table()).isEqualTo("inventory");
        assertThat(e.lsn()).isEqualTo(77L);
    }

    @Test
    void nullValueIsTombstone() {
        assertThat(parser.parse(null)).isNull();
    }

    @Test
    void lsnIsMonotonicAcrossSequence() {
        long c = parse("""
                {"after":{"id":1},"source":{"table":"orders","lsn":1000,"ts_ms":1},"op":"c","ts_ms":1}
                """).lsn();
        long u = parse("""
                {"before":{"id":1},"after":{"id":1},"source":{"table":"orders","lsn":2000,"ts_ms":2},"op":"u","ts_ms":2}
                """).lsn();
        long d = parse("""
                {"before":{"id":1},"source":{"table":"orders","lsn":3000,"ts_ms":3},"op":"d","ts_ms":3}
                """).lsn();
        assertThat(c).isLessThan(u);
        assertThat(u).isLessThan(d);
    }
}
