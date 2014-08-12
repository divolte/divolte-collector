package io.divolte.server.hdfs;

import static io.divolte.server.ConcurrentUtils.*;
import io.divolte.server.AvroRecordBuffer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

@ParametersAreNonnullByDefault
final class HdfsFlusher {
    private final static Logger logger = LoggerFactory.getLogger(HdfsFlusher.class);

    private final BlockingQueue<AvroRecordBuffer> queue;
    private final long maxEnqueueDelayMillis;

    private final FileSystem hadoopFs;
    private final String hdfsFileDir;
    private final short hdfsReplication;
    private final long syncEveryMillis;
    private final int syncEveryRecords;

    private final static SimpleDateFormat datePartFormat = new SimpleDateFormat("yyyyLLddHHmmssSSS");

    private final Schema schema;

    private HadoopFile currentFile;
    private boolean isHdfsAlive;
    private long lastFixAttempt;

    public HdfsFlusher(final Config config, final Schema schema) {
        this.schema = schema;

        queue = new LinkedBlockingQueue<>(config.getInt("divolte.hdfs_flusher.max_write_queue"));
        maxEnqueueDelayMillis = config.getDuration("divolte.hdfs_flusher.max_enqueue_delay", TimeUnit.MILLISECONDS);

        syncEveryMillis = config.getDuration("divolte.hdfs_flusher.sync_file_after_duration", TimeUnit.MILLISECONDS);
        syncEveryRecords = config.getInt("divolte.hdfs_flusher.sync_file_after_records");

        try {
            hdfsFileDir = config.getString("divolte.hdfs_flusher.dir");
            hdfsReplication = (short) config.getInt("divolte.hdfs_flusher.hdfs.replication");
            URI hdfsLocation = new URI(config.getString("divolte.hdfs_flusher.hdfs.uri"));
            hadoopFs = FileSystem.get(hdfsLocation, new Configuration());
        } catch (IOException|URISyntaxException e) {
            /*
             * It is possible to create a FileSystem instance when HDFS is not available (e.g. NameNode down).
             * This exception only occurs when there is a configuration error in the URI (e.g. wrong scheme).
             * So we fail to start up in this case. Below we create the actual HDFS connection, by opening
             * files. If that fails, we do startup and initiate the regular retry cycle.
             */
            logger.error("Could not initialize HDFS filesystem.", e);
            throw new RuntimeException("Could not initialize HDFS filesystem", e);
        }

        final Path newFilePath = newFilePath();
        try {
            currentFile = openNewFile(newFilePath);
            isHdfsAlive = true;
        } catch (IOException e) {
            logger.warn("HDFS flusher starting up without HDFS connection.", e);
            isHdfsAlive = false;
            // possibly we created the file, but found it wasn't writable; hence we attempt a delete
            throwsIoException(() -> hadoopFs.delete(newFilePath, false));
        }
    }

    public Runnable getQueueReader() {
        return microBatchingQueueDrainerWithHeartBeat(queue, this::processRecord, this::heartBeat);
    }

    private void doProcess(AvroRecordBuffer record) throws IllegalArgumentException, IOException {
        if (isHdfsAlive) {
            currentFile.writer.appendEncoded(record.getByteBuffer());
            currentFile.recordsSinceLastSync += 1;

            possiblySync();
        } else {
            possiblyFixHdfsConnection();
        }
    }

    private void doHeartBeat() throws IOException {
        if (isHdfsAlive) {
            possiblySync();
        } else {
            possiblyFixHdfsConnection();
        }
    }

    private void possiblyFixHdfsConnection() {
        // try to reconnect every 5 seconds.
        long time = System.currentTimeMillis();
        if (time - lastFixAttempt > 5000) {
            final Path newFilePath = newFilePath();
            try {
                currentFile = openNewFile(newFilePath);
                isHdfsAlive = true;
                lastFixAttempt = 0;
            } catch (IOException ioe) {
                logger.warn("Could not create HDFS file.", ioe);
                isHdfsAlive = false;
                lastFixAttempt = time;
                throwsIoException(() -> hadoopFs.delete(newFilePath, false));
            }
        }
    }

    private Path newFilePath() {
        return new Path(hdfsFileDir, String.format("%s-divolte-tracking-%d.avro", datePartFormat.format(new Date()), hashCode()));
    }

    private HadoopFile openNewFile(Path path) throws IOException {
        return new HadoopFile(path);
    }

    private void possiblySync() throws IOException {
        final long time = System.currentTimeMillis();
        if (
                currentFile.recordsSinceLastSync > syncEveryRecords ||
                (time - currentFile.lastSyncTime > syncEveryMillis && currentFile.recordsSinceLastSync > 0)) {
            logger.debug("Syncing HDFS file: {}", currentFile.path.getName());

            // Forces the Avro file to write a block
            currentFile.writer.sync();
            // Forces a (HDFS) sync on the underlying stream
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

    public void add(AvroRecordBuffer record) {
        if (!offerQuietly(queue, record, maxEnqueueDelayMillis, TimeUnit.MILLISECONDS)) {
            logger.warn("Dropping record on attempt to enqueue for HDFS flushing.");
        }
    }

    public void cleanup() {
        try {
            doCleanup();
        } catch (IOException e) {
            logger.warn("Failed to cleanly close HDFS file.", e);
            isHdfsAlive = false;
        }
    }

    private void processRecord(AvroRecordBuffer record) {
        try {
            doProcess(record);
        } catch (IOException e) {
            logger.warn("Failed to flush record to HDFS.", e);
            isHdfsAlive = false;
        }
    }

    private void heartBeat() {
        try {
            doHeartBeat();
        } catch (IOException e) {
            logger.warn("Failed to flush record to HDFS.", e);
            isHdfsAlive = false;
        }
    }

    private final class HadoopFile implements AutoCloseable {
        final Path path;
        final FSDataOutputStream stream;
        final DataFileWriter<GenericRecord> writer;

        long lastSyncTime;
        int recordsSinceLastSync;

        @SuppressWarnings("resource")
        public HadoopFile(Path path) throws IOException {
            this.path = path;
            this.stream = hadoopFs.create(path, hdfsReplication);

            this.writer =
                    new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))
                    .create(schema, stream);
            this.writer.setSyncInterval(1 << 30);
            this.writer.setFlushOnEveryBlock(true);

            // Sync the file on open to make sure the
            // connection actually works, because
            // HDFS allows file creation even with no
            // datanodes available
            this.stream.hsync();

            this.lastSyncTime = System.currentTimeMillis();
            this.recordsSinceLastSync = 0;
        }

        public void close() throws IOException { writer.close(); }
    }
}
