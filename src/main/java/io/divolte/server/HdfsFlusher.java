package io.divolte.server;

import static io.divolte.server.ConcurrentUtils.*;
import io.divolte.record.IncomingRequestRecord;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

final class HdfsFlusher {
    private final static Logger logger = LoggerFactory.getLogger(HdfsFlusher.class);

    private final LinkedBlockingQueue<AvroRecordBuffer<SpecificRecord>> queue;
    private final long maxEnqueueDelayMillis;

    private final FileSystem hadoopFs;
    private final String hdfsFileDir;
    private final short hdfsReplication;
    private final long syncEveryMillis;
    private final int syncEveryRecords;

    private HadoopFile currentFile;

    public HdfsFlusher(Config config) {
        queue = new LinkedBlockingQueue<>(config.getInt("divolte.hdfs_flusher.max_write_queue"));
        maxEnqueueDelayMillis = config.getDuration("divolte.hdfs_flusher.max_enqueue_delay", TimeUnit.MILLISECONDS);

        syncEveryMillis = config.getDuration("divolte.hdfs_flusher.sync_file_after_duration", TimeUnit.MILLISECONDS);
        syncEveryRecords = config.getInt("divolte.hdfs_flusher.sync_file_after_records");

        URI hdfsLocation;
        try {
            hdfsFileDir = config.getString("divolte.hdfs_flusher.dir");
            hdfsReplication = (short) config.getInt("divolte.hdfs_flusher.hdfs.replication");
            hdfsLocation = new URI(config.getString("divolte.hdfs_flusher.hdfs.uri"));
            hadoopFs = FileSystem.get(hdfsLocation, new Configuration());

            currentFile = new HadoopFile(new Path(hdfsFileDir, "divolte-tracking-" + hashCode() + ".avro"));
        } catch (Exception e) {
            logger.error("Could not initialize HDFS connection.");
            System.exit(1);
            throw new RuntimeException();
        }
    }

    public Runnable getQueueReader() {
        return microBatchingQueueDrainerWithHeartBeat(queue, this::processRecord, this::heartBeat);
    }

    private void doProcess(AvroRecordBuffer<SpecificRecord> record) throws IllegalArgumentException, IOException {
        currentFile.writer.appendEncoded(record.getBufferSlice());
        currentFile.recordsSinceLastSync += 1;

        possiblySync();
    }

    private void doHeartBeat() throws IOException {
        possiblySync();
    }

    private void possiblySync() throws IOException {
        final long time = System.currentTimeMillis();
        if (
                currentFile.recordsSinceLastSync > syncEveryRecords ||
                (time - currentFile.lastSyncTime > syncEveryMillis && currentFile.recordsSinceLastSync > 0)) {
            logger.debug("Syncing HDFS file: {}", currentFile.path.getName());

            // Forces the Avro file to write a block
            currentFile.writer.sync();
            // Forces a sync on the underlying stream
            currentFile.stream.hsync();

            currentFile.recordsSinceLastSync = 0;
            currentFile.lastSyncTime = time;
        } else if (currentFile.recordsSinceLastSync == 0) {
            currentFile.lastSyncTime = time;
        }
    }

    private void doCleanup() throws IOException {
        logger.debug("Closing HDFS file for cleanup: {}", currentFile.path.getName());
        currentFile.close();
    }

    public void add(AvroRecordBuffer<SpecificRecord> record) {
        if (!offerQuietly(queue, record, maxEnqueueDelayMillis, TimeUnit.MILLISECONDS)) {
            //TODO: failed to enqueue spill locally
            logger.warn("Dropping record on attempt to enqueue for HDFS flushing.");
        }
    }

    public void cleanup() {
        logger.debug("Cleanup.");
        try {
            doCleanup();
        } catch (IOException e) {
            logger.error("TODO: Handle HDFS exception.", e);
            System.exit(1);
        }
    }

    private void processRecord(AvroRecordBuffer<SpecificRecord> record) {
        try {
            doProcess(record);
        } catch (IllegalArgumentException e) {
            logger.error("TODO: Handle HDFS exception.", e);
            System.exit(1);
        } catch (IOException e) {
            logger.error("TODO: Handle HDFS exception.", e);
            System.exit(1);
        }
    }

    private void heartBeat() {
        try {
            doHeartBeat();
        } catch (IOException e) {
            logger.error("TODO: Handle HDFS exception.", e);
            System.exit(1);
        }
    }

    // with the outlook of having to manage multiple HDFS files
    // it seemed to make sense to create this structure
    private final class HadoopFile {
        final Path path;
        final FSDataOutputStream stream;
        final DataFileWriter<SpecificRecord> writer;

        long lastSyncTime;
        int recordsSinceLastSync;

        @SuppressWarnings("resource")
        public HadoopFile(Path path) throws IOException {
            this.path = path;
            this.stream = hadoopFs.create(path, hdfsReplication);

            this.writer = new DataFileWriter<SpecificRecord>(
                    new SpecificDatumWriter<>(IncomingRequestRecord.SCHEMA$)).create(IncomingRequestRecord.SCHEMA$,
                            stream);
            this.writer.setSyncInterval(1 << 30);
            this.writer.setFlushOnEveryBlock(true);

            lastSyncTime = System.currentTimeMillis();
            recordsSinceLastSync = 0;
        }

        void close() throws IOException { writer.close(); }
    }
}
