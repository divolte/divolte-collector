package io.divolte.server.hdfs;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.processing.ProcessingPool;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.avro.Schema;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@ParametersAreNonnullByDefault
public final class HdfsFlushingPool extends ProcessingPool<HdfsFlusher, AvroRecordBuffer>{
    public HdfsFlushingPool(final Schema schema) {
        this(ConfigFactory.load(), schema);
    }

    public HdfsFlushingPool(final Config config, final Schema schema) {
        this(
                Objects.requireNonNull(config),
                Objects.requireNonNull(schema),
                config.getInt("divolte.hdfs_flusher.threads"),
                config.getInt("divolte.hdfs_flusher.max_write_queue"),
                config.getDuration("divolte.hdfs_flusher.max_enqueue_delay", TimeUnit.MILLISECONDS)
                );
    }

    public HdfsFlushingPool(final Config config, final Schema schema, final int numThreads, final int maxQueueSize, final long maxEnqueueDelay) {
        super(
                numThreads,
                maxQueueSize,
                maxEnqueueDelay,
                "Hdfs Flusher",
                () -> new HdfsFlusher(config, schema));
    }

    public void enqueueRecordsForFlushing(final AvroRecordBuffer record)  {
        enqueue(record.getPartyId().getValue(), record);
    }
}
