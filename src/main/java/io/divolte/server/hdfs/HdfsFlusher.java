package io.divolte.server.hdfs;

import static io.divolte.server.ConcurrentUtils.*;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.processing.ItemProcessor;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
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
final class HdfsFlusher implements ItemProcessor<AvroRecordBuffer> {
    private static final int HDFS_RECONNECT_DELAY = 5000;

    private final static Logger logger = LoggerFactory.getLogger(HdfsFlusher.class);

    private final DateFormat datePartFormat = new SimpleDateFormat("yyyyLLddHHmmssSSS");

    private final FileSystem hadoopFs;
    private final String hdfsFileDir;
    private final short hdfsReplication;
    private final long syncEveryMillis;
    private final int syncEveryRecords;

    private final Schema schema;

    private HadoopFile currentFile;
    private boolean isHdfsAlive;
    private long lastFixAttempt;

    public HdfsFlusher(final Config config, final Schema schema) {
        Objects.requireNonNull(config);
        this.schema = Objects.requireNonNull(schema);

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

    private void doProcess(AvroRecordBuffer record) throws IllegalArgumentException, IOException {
        if (isHdfsAlive) {
            currentFile.writer.appendEncoded(record.getByteBuffer());
            currentFile.recordsSinceLastSync += 1;

            possiblySync();
        } else {
            possiblyFixHdfsConnection();
        }
    }

    private void doHeartbeat() throws IOException {
        if (isHdfsAlive) {
            possiblySync();
        } else {
            possiblyFixHdfsConnection();
        }
    }

    private void possiblyFixHdfsConnection() {
        // try to reconnect every 5 seconds.
        long time = System.currentTimeMillis();
        if (time - lastFixAttempt > HDFS_RECONNECT_DELAY) {
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
                time - currentFile.lastSyncTime > syncEveryMillis && currentFile.recordsSinceLastSync > 0) {
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

    @Override
    public void cleanup() {
        try {
            doCleanup();
        } catch (IOException e) {
            logger.warn("Failed to cleanly close HDFS file.", e);
            isHdfsAlive = false;
        }
    }

    @Override
    public void process(AvroRecordBuffer record) {
        try {
            doProcess(record);
        } catch (IOException e) {
            logger.warn("Failed to flush record to HDFS.", e);
            isHdfsAlive = false;
        }
    }

    @Override
    public void heartbeat() {
        try {
            doHeartbeat();
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
