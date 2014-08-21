package io.divolte.server.hdfs;

import static io.divolte.server.hdfs.FileCreateAndSyncStrategy.HdfsOperationResult.*;
import io.divolte.server.AvroRecordBuffer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

@NotThreadSafe
@ParametersAreNonnullByDefault
public class SimpleRollingFileStrategy implements FileCreateAndSyncStrategy {
    private static final Logger logger = LoggerFactory.getLogger(SimpleRollingFileStrategy.class);

    private final static long HDFS_RECONNECT_DELAY = 15000;

    private final static AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    private final int instanceNumber;
    private final String hostString;
    private final DateFormat datePartFormat = new SimpleDateFormat("yyyyLLddHHmmss");

    private final Schema schema;

    private final long syncEveryMillis;
    private final int syncEveryRecords;
    private final long newFileEveryMillis;

    private final FileSystem hadoopFs;
    private final String hdfsFileDir;
    private final short hdfsReplication;

    private HadoopFile currentFile;
    private boolean isHdfsAlive;
    private long lastFixAttempt;

    public SimpleRollingFileStrategy(final Config config, final FileSystem fs, final short hdfsReplication, final Schema schema) {
        Objects.requireNonNull(config);
        this.schema = Objects.requireNonNull(schema);

        syncEveryMillis = config.getDuration("divolte.hdfs_flusher.simple_rolling_file_strategy.sync_file_after_duration", TimeUnit.MILLISECONDS);
        syncEveryRecords = config.getInt("divolte.hdfs_flusher.simple_rolling_file_strategy.sync_file_after_records");
        newFileEveryMillis = config.getDuration("divolte.hdfs_flusher.simple_rolling_file_strategy.roll_every", TimeUnit.MILLISECONDS);

        instanceNumber = INSTANCE_COUNTER.incrementAndGet();
        hostString = findLocalHostName();

        this.hadoopFs = fs;
        this.hdfsReplication = hdfsReplication;

        hdfsFileDir = config.getString("divolte.hdfs_flusher.simple_rolling_file_strategy.dir");
    }

    private String findLocalHostName() {
        // we should use the bind address from the divolte.server config to figure out the actual hostname we are listening on
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

    private Path newFilePath() {
        return new Path(hdfsFileDir, String.format("%s-divolte-tracking-%s-%d.avro", datePartFormat.format(new Date()), hostString, instanceNumber));
    }

    private HadoopFile openNewFile(final Path path) throws IOException {
        return new HadoopFile(path);
    }

    @Override
    public HdfsOperationResult setup() {
        final Path newFilePath = newFilePath();

        return throwsIoException(() -> currentFile = openNewFile(newFilePath))
        .map((ioe) -> {
            logger.warn("HDFS flusher starting up without HDFS connection.", ioe);
            isHdfsAlive = false;
            throwsIoException(() -> hadoopFs.delete(newFilePath, false));
            return FAILURE;
        })
        .orElseGet(() -> {
            isHdfsAlive = true;
            logger.debug("Created new file in setup: {}", newFilePath.toString());
            return SUCCESS;
        });
    }

    @Override
    public HdfsOperationResult heartbeat() {
        if (isHdfsAlive) {
            return throwsIoException(this::possiblySync).map((ioe) -> {
                isHdfsAlive = false;
                return FAILURE;
            }).orElse(SUCCESS);
        } else {
            return possiblyFixHdfsConnection();
        }
    }

    @Override
    public HdfsOperationResult append(final AvroRecordBuffer record) {
        if (!isHdfsAlive) {
            throw new IllegalStateException("Append attempt while HDFS connection is not alive.");
        }

        return throwsIoException(() -> {
            currentFile.writer.appendEncoded(record.getByteBuffer());
            currentFile.recordsSinceLastSync += 1;
            possiblySync();
        }).map((ioe) -> {
            logger.warn("Failed to flush event to HDFS.", ioe);
            isHdfsAlive = false;
            return FAILURE;
        }).orElse(SUCCESS);
    }

    @Override
    public void cleanup() {
        throwsIoException(currentFile::close)
        .ifPresent((ioe) -> logger.warn("Failed to close HDFS file on flusher cleanup.", ioe));
    }

    private void possiblySync() throws IOException {
        final long time = System.currentTimeMillis();

        if (
                currentFile.recordsSinceLastSync >= syncEveryRecords ||
                time - currentFile.lastSyncTime >= syncEveryMillis && currentFile.recordsSinceLastSync > 0) {
            logger.debug("Syncing HDFS file: {}", currentFile.path.toString());

            // Forces the Avro file to write a block
            currentFile.writer.sync();
            // Forces a (HDFS) sync on the underlying stream
            currentFile.stream.hsync();

            currentFile.recordsSinceLastSync = 0;
            currentFile.lastSyncTime = time;
            possiblyRollFile(time);
        } else if (currentFile.recordsSinceLastSync == 0) {
            currentFile.lastSyncTime = time;
            possiblyRollFile(time);
        }
    }

    private void possiblyRollFile(final long time) throws IOException {
        if (time > currentFile.projectedCloseTime) {
            currentFile.close();

            logger.debug("Rolling file. Closed: {}", currentFile.path);

            final Path newFilePath = newFilePath();
            try {
                currentFile = openNewFile(newFilePath);
            } catch (IOException e) {
                throwsIoException(() -> hadoopFs.delete(newFilePath, false));
                throw e;
            }
            logger.debug("Rolling file. Opened: {}", currentFile.path);
        }
    }

    private HdfsOperationResult possiblyFixHdfsConnection() {
        if (isHdfsAlive) {
            throw new IllegalStateException("HDFS connection repair attempt while not broken.");
        }

        long time = System.currentTimeMillis();
        if (time - lastFixAttempt > HDFS_RECONNECT_DELAY) {
            final Path newFilePath = newFilePath();
            return throwsIoException(() -> {
                currentFile = openNewFile(newFilePath);
            }).map((ioe) -> {
                isHdfsAlive = false;
                lastFixAttempt = time;
                // possibly we created the file, so silently attempt a delete
                throwsIoException(() -> hadoopFs.delete(newFilePath, false));
                return FAILURE;
            }).orElseGet(() -> {
                isHdfsAlive = true;
                lastFixAttempt = 0;
                logger.info("Recovered HDFS connection.");
                return SUCCESS;
            });
        } else {
            return FAILURE;
        }
    }

    private final class HadoopFile implements AutoCloseable {
        final long openTime;
        final long projectedCloseTime;
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

            this.openTime = this.lastSyncTime = System.currentTimeMillis();
            this.recordsSinceLastSync = 0;
            this.projectedCloseTime = openTime + newFileEveryMillis;
        }

        public void close() throws IOException { writer.close(); }
    }

    @FunctionalInterface
    public interface IOExceptionThrower {
        public abstract void run() throws IOException;
    }

    private static Optional<IOException> throwsIoException(final IOExceptionThrower r) {
        try {
            r.run();
            return Optional.empty();
        } catch (final IOException ioe) {
            return Optional.of(ioe);
        }
    }
}
