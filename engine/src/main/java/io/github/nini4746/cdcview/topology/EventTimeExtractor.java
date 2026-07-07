package io.github.nini4746.cdcview.topology;

import io.github.nini4746.cdcview.cdc.ChangeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * event time = 커밋 시각(source.ts_ms). 처리 시각이 아니다(윈도우/지연 측정 기준, SPEC 확정).
 */
public class EventTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof ChangeEvent ev && ev.tsMs() > 0) {
            return ev.tsMs();
        }
        // 파싱 실패/tombstone 등은 파티션 시각으로 대체(음수 방지)
        return partitionTime < 0 ? 0 : partitionTime;
    }
}
