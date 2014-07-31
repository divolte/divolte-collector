package io.divolte.server;

import static io.divolte.server.ConcurrentUtils.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;
import com.typesafe.config.Config;

final class LocalFileFlusher {
    private final static Logger logger = LoggerFactory.getLogger(LocalFileFlusher.class);

    private final static SimpleDateFormat datePartFormat = new SimpleDateFormat("yyyyLLddHHmmssSSS");

    private final long maxEnqueueDelayMillis;
    private final LinkedBlockingQueue<AvroRecordBufferWithSequenceNumber> queue;

    private final String localFilesDirectory;

    private final AtomicLong position;
    private FileChannel channel;


    public LocalFileFlusher(Config config) {
        queue = new LinkedBlockingQueue<>(config.getInt("divolte.local_file_flusher.max_write_queue"));
        maxEnqueueDelayMillis = config.getDuration("divolte.local_file_flusher.max_enqueue_delay", TimeUnit.MILLISECONDS);

        position = new AtomicLong();

        localFilesDirectory = config.getString("divolte.local_file_flusher.dir");
        closeExistingAndCreateNewFileChannel();
    }

    public Runnable getQueueReader() {
        return microBatchingQueueDrainer(queue, this::processRecord);
    }

    public void processRecord(AvroRecordBufferWithSequenceNumber record) {
        try {
            logger.debug("Flushing record locally with sequence number {} at position {}", record.sequenceNumber, channel.position());

            final ByteBuffer[] buffersToWrite = {
                    ByteBuffer.wrap(Ints.toByteArray(record.sequenceNumber)),
                    ByteBuffer.wrap(Ints.toByteArray(record.record.size())),
                    record.record.getBufferSlice()
                    };

            channel.write(buffersToWrite);
        } catch (IOException e) {
            logger.error("Failed to write to local file.", e);
        }
    }

    public void close() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException e) {
                logger.warn("Error while closing file channel.", e);
            }
        }

        channel = null;
    }

    public long add(int sequenceNumber, AvroRecordBuffer<SpecificRecord> record) {
        final long currentPosition = position.get();

        if (offerQuietly(queue, new AvroRecordBufferWithSequenceNumber(sequenceNumber, record), maxEnqueueDelayMillis, TimeUnit.MILLISECONDS)) {
            position.addAndGet(record.size());
        } else {
            logger.warn("Failed to enqueue event with sequence number {} for local disk flushing after {} ms. Dropping event locally.", sequenceNumber, maxEnqueueDelayMillis);
        }

        return currentPosition;
    }

    private void closeExistingAndCreateNewFileChannel() {
        try {
            final long position;
            if (channel != null && channel.isOpen()) {
                position = channel.position();
                channel.force(true);
                channel.close();
            } else {
                position = 0;
            }

            final Path path = FileSystems.getDefault().getPath(localFilesDirectory, String.format("%s-%d-%d-divolte-collector-data.bin", datePartFormat.format(new Date()), hashCode(), position));
            channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            logger.error("IO error while attempting to create local data file.");
            throw new RuntimeException("IO error while attempting to create local data file.", e);
        }
    }

    private final class AvroRecordBufferWithSequenceNumber {
        final AvroRecordBuffer<SpecificRecord> record;
        final int sequenceNumber;

        public AvroRecordBufferWithSequenceNumber(int sequenceNumber, AvroRecordBuffer<SpecificRecord> record) {
            this.record = record;
            this.sequenceNumber = sequenceNumber;
        }
    }
}

