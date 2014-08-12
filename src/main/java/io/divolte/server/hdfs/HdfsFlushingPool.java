package io.divolte.server.hdfs;

import io.divolte.server.AvroRecordBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.avro.specific.SpecificRecord;

import static io.divolte.server.ConcurrentUtils.*;

public final class HdfsFlushingPool {
    private final List<HdfsFlusher> flushers;

    public HdfsFlushingPool() {
        this(ConfigFactory.load());
    }

    public HdfsFlushingPool(final Config config) {
        final int numThreads = config.getInt("divolte.hdfs_flusher.threads");

        final ThreadGroup threadGroup = new ThreadGroup("Hdfs Flushing Pool");
        final ThreadFactory factory = createThreadFactory(threadGroup, "Hdfs Flusher - %d");
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads, factory);

        flushers = Stream.generate(() -> new HdfsFlusher(config))
                         .limit(numThreads)
                         .collect(Collectors.toCollection(() -> new ArrayList<>(numThreads)));

        flushers.forEach((flusher) ->
            scheduleQueueReaderWithCleanup(
                    executorService,
                    flusher.getQueueReader(),
                    flusher::cleanup)
        );
    }

    public void enqueueRecordsForFlushing(final AvroRecordBuffer<SpecificRecord> record)  {
        final int bucket = (record.getPartyId().hashCode() & Integer.MAX_VALUE) % flushers.size();
        flushers.get(bucket).add(record);
    }
}
