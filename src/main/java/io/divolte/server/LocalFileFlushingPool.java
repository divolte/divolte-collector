package io.divolte.server;

import static io.divolte.server.ConcurrentUtils.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

final class LocalFileFlushingPool {
    private final static Logger logger = LoggerFactory.getLogger(LocalFileFlushingPool.class);

    private final AtomicInteger sequenceNumber;
    private final List<LocalFileFlusher> flushers;

    public LocalFileFlushingPool() {
        this(ConfigFactory.load());
    }

    public LocalFileFlushingPool(final Config config) {
        final int numFlusherThreads = config.getInt("divolte.local_file_flusher.threads");

        final ThreadGroup threadGroup = new ThreadGroup("Local File Flushing Pool");
        final ThreadFactory factory = createThreadFactory(threadGroup, "Local File Flusher - %d");
        final ExecutorService executorService = Executors.newFixedThreadPool(numFlusherThreads, factory);

        sequenceNumber = new AtomicInteger();

        flushers = Stream.generate(() -> new LocalFileFlusher(config))
                .limit(numFlusherThreads)
                .collect(Collectors.toCollection(() -> new ArrayList<>(numFlusherThreads)));

        flushers.forEach((flusher) -> {
            scheduleQueueReaderWithCleanup(executorService, flusher.getQueueReader(), flusher::close);
        });
    }

    public FilePosition enqueueRecordForFlushing(final String partyId, final AvroRecordBuffer<SpecificRecord> record) {
        final int seq = this.sequenceNumber.incrementAndGet();
        final int bucket = (partyId.hashCode() & Integer.MAX_VALUE) % flushers.size();
        final long position = flushers.get(bucket).add(seq, record);
        return new FilePosition(seq, position);
    }

    public final class FilePosition {
        final int sequenceNumber;
        final long filePosition;

        public FilePosition(int sequenceNumber, long filePosition) {
            this.sequenceNumber = sequenceNumber;
            this.filePosition = filePosition;
        }
    }
}

