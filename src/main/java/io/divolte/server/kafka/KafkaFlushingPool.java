package io.divolte.server.kafka;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.processing.ProcessingPool;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.ParametersAreNonnullByDefault;

import com.typesafe.config.Config;

@ParametersAreNonnullByDefault
public class KafkaFlushingPool extends ProcessingPool<KafkaFlusher, AvroRecordBuffer> {
    public KafkaFlushingPool(final Config config) {
        this(
                Objects.requireNonNull(config),
                config.getInt("divolte.kafka_flusher.threads"),
                config.getInt("divolte.kafka_flusher.max_write_queue"),
                config.getDuration("divolte.kafka_flusher.max_enqueue_delay", TimeUnit.MILLISECONDS)
                );
    }

    public KafkaFlushingPool(Config config, int numThreads, int maxWriteQueue, long maxEnqueueDelay) {
        super(numThreads, maxWriteQueue, maxEnqueueDelay, "Kafka Flusher", () -> new KafkaFlusher(config));
    }

    public void enqueueRecord(final AvroRecordBuffer record) {
        enqueue(record.getPartyId(), record);
    }
}
