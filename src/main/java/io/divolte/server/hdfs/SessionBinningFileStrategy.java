package io.divolte.server.hdfs;

import static io.divolte.server.hdfs.FileCreateAndSyncStrategy.HdfsOperationResult.*;
import static java.util.Calendar.*;
import io.divolte.server.AvroRecordBuffer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

import com.google.common.collect.Maps;
import com.typesafe.config.Config;

@NotThreadSafe
public class SessionBinningFileStrategy implements FileCreateAndSyncStrategy {
    private final static Logger logger = LoggerFactory.getLogger(SessionBinningFileStrategy.class);

    private final static AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    private final int instanceNumber;
    private final String hostString;


    private final FileSystem hdfs;
    private final short hdfsReplication;

    private final Schema schema;

    private final long sessionTimeoutMillis;

    private final Map<Long, RoundHdfsFile> openFiles;
    private final String hdfsFileDir;
    private final long syncEveryMillis;
    private final int syncEveryRecords;

    private boolean isHdfsAlive;
    private long timeSignal;


    public SessionBinningFileStrategy(final Config config, final FileSystem hdfs, final short hdfsReplication, final Schema schema) {
        sessionTimeoutMillis = config.getDuration("divolte.tracking.session_timeout", TimeUnit.MILLISECONDS);

        hostString = findLocalHostName();
        instanceNumber = INSTANCE_COUNTER.incrementAndGet();
        hdfsFileDir = config.getString("divolte.hdfs_flusher.session_binning_file_strategy.dir");

        syncEveryMillis = config.getDuration("divolte.hdfs_flusher.session_binning_file_strategy.sync_file_after_duration", TimeUnit.MILLISECONDS);
        syncEveryRecords = config.getInt("divolte.hdfs_flusher.session_binning_file_strategy.sync_file_after_records");

        this.hdfs = hdfs;
        this.hdfsReplication = hdfsReplication;

        this.schema = schema;

        openFiles = Maps.newHashMapWithExpectedSize(10);
    }

    private static String findLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

    @Override
    public HdfsOperationResult setup() {
        return SUCCESS;
    }

    @Override
    public HdfsOperationResult heartbeat() {
        timeSignal = System.currentTimeMillis();

        if (isHdfsAlive) {
            return throwsIoException(this::possiblySync)
            .map((ioe) -> {
                isHdfsAlive = false;
                return FAILURE;
            })
            .orElse(SUCCESS);
        } else {
            return possiblyFixHdfsConnection();
        }
    }

    @Override
    public HdfsOperationResult append(AvroRecordBuffer record) {
        if (!isHdfsAlive) {
            throw new IllegalStateException("Append attempt while HDFS connection is not alive.");
        }

        timeSignal = record.getEventTime();
        return throwsIoException(() -> {
            RoundHdfsFile file = fileForSessionStartTime(record.getSessionId().timestamp);
            file.writer.appendEncoded(record.getByteBuffer());
            file.recordsSinceLastSync += 1;
            possiblySync(file);
        })
        .map((ioe) -> {
            logger.warn("Error while flushing event to HDFS.", ioe);
            isHdfsAlive = false;
            return FAILURE;
        })
        .orElse(SUCCESS);
    }

    @Override
    public void cleanup() {
    }

    private void possiblySync(RoundHdfsFile file) {
        try {
            final long time = System.currentTimeMillis();

            if (
                    file.recordsSinceLastSync >= syncEveryRecords ||
                    time - file.lastSyncTime >= syncEveryMillis && file.recordsSinceLastSync > 0) {
                logger.debug("Syncing HDFS file: {}", file.path.toString());

                // Forces the Avro file to write a block
                    file.writer.sync();
                // Forces a (HDFS) sync on the underlying stream
                file.stream.hsync();

                file.recordsSinceLastSync = 0;
                file.lastSyncTime = time;
            } else if (file.recordsSinceLastSync == 0) {
                file.lastSyncTime = time;
            }
        } catch (IOException e) {
            throw new WrappedIOException(e);
        }
    }

    private void possiblySync() {
        /*
         * This only happens on hearbeat calls, so since there is nothing to do
         * we may as well iterate all open files and possibly sync
         */
        openFiles.values().forEach(this::possiblySync);
    }

    private HdfsOperationResult possiblyFixHdfsConnection() {
        isHdfsAlive = true;
        return SUCCESS;
    }

    private RoundHdfsFile fileForSessionStartTime(final long sessionStartTime) {
        return openFiles.computeIfAbsent(sessionStartTime / sessionTimeoutMillis, (startTime) -> {
            // return the first open file for which the round >= the requested round
            // or create a new file if no such file is present
            final long requestedRound = sessionStartTime / sessionTimeoutMillis;
            return openFiles
                .values()
                .stream()
                .sorted((left, right) -> Long.compare(left.round, right.round))
                .filter((f) -> f.round >= requestedRound)
                .findFirst()
                .orElse(new RoundHdfsFile(sessionStartTime));
        });
    }

    private final class RoundHdfsFile implements AutoCloseable {
        final Path path;
        final long round;
        final FSDataOutputStream stream;
        final DataFileWriter<GenericRecord> writer;

        long lastSyncTime;
        int recordsSinceLastSync;

        RoundHdfsFile(final long time) {
            final long requestedRound = time / sessionTimeoutMillis;
            final long oldestAllowedRound = (timeSignal / sessionTimeoutMillis) - 2;
            this.round = Math.max(requestedRound, oldestAllowedRound);

            this.path = new Path(hdfsFileDir, String.format("%s-divolte-tracking-%s-%d.avro", hostString, roundString(round * sessionTimeoutMillis), instanceNumber));

            try {

                stream = hdfs.create(path, hdfsReplication);
                writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema)).create(schema, stream);
                writer.setSyncInterval(1 << 30);
                writer.setFlushOnEveryBlock(true);

                // Sync the file on open to make sure the
                // connection actually works, because
                // HDFS allows file creation even with no
                // datanodes available
                stream.hsync();

                lastSyncTime = System.currentTimeMillis();
                recordsSinceLastSync = 0;

                logger.debug("Created new HDFS file: {}", path);
            } catch (IOException e) {
                logger.warn("Failed HDFS file creation: {}", path);
                // we may have created the file, but failed to sync, so we attempt a delete
                // this happens when the NN responds successfully, but there are no DNs available
                throwsIoException(() -> hdfs.delete(path, false));
                throw new WrappedIOException(e);
            }
        }

        private String roundString(final long roundStartTime) {
            final GregorianCalendar gc = new GregorianCalendar();
            gc.setTimeInMillis(roundStartTime);
            gc.set(HOUR_OF_DAY, 0);
            gc.set(MINUTE, 0);
            gc.set(SECOND, 0);
            gc.set(MILLISECOND, 0);

            return String.format("%d%02d%02d-%02d",
                    gc.get(YEAR),
                    gc.get(MONTH),
                    gc.get(DAY_OF_MONTH),
                    (roundStartTime - gc.getTimeInMillis()) / sessionTimeoutMillis);
        }

        public void close() { try { writer.close(); } catch (IOException e) { throw new WrappedIOException(e); } }
    }

    private final class WrappedIOException extends RuntimeException {
        private static final long serialVersionUID = -4052372882422830902L;
        final IOException wrappedIOException;

        private WrappedIOException(IOException ioe) {
            this.wrappedIOException = ioe;
        }
    }

    @FunctionalInterface
    private interface IOExceptionThrower {
        public abstract void run() throws IOException;
    }

    private static Optional<IOException> throwsIoException(final IOExceptionThrower r) {
        try {
            r.run();
            return Optional.empty();
        } catch (final IOException ioe) {
            return Optional.of(ioe);
        } catch (final WrappedIOException wioe) {
            return Optional.of(wioe.wrappedIOException);
        }
    }
}
