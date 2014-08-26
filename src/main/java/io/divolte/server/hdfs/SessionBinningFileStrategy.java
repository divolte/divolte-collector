package io.divolte.server.hdfs;

import static io.divolte.server.hdfs.FileCreateAndSyncStrategy.HdfsOperationResult.*;
import static java.util.Calendar.*;
import io.divolte.server.AvroRecordBuffer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

@NotThreadSafe
/*
 * The general idea of this file strategy is to provide a best effort to put events that belong to the same session in the same file.
 *
 * The session binning file strategy assigns event to files as such:
 * - each timestamp is assigned to a round, defined as timestamp_in_millis / session_timeout_in_millis
 * - we open a file for a round as time passes
 * - all events for a session are stored in the file with the round marked by the session start time
 * - a file for a round is kept open for at least three times the session duration *in absence of failures*
 * - during this entire process, we use the event timestamp for events that come off the queue as a logical clock signal
 *      - only in the case of an empty queue, we use the actual system time as clock signal (receiving heartbeats means an empty queue)
 * - when a file for a round is closed, but events that should be in that file still arrive, they are stored in the oldest open file
 *      - this happens for exceptionally long sessions
 *
 * The above mechanics allow for the following guarantee: if a file is properly opened, used for flushing and closed without intermediate failures,
 * all sessions that start within that file and last less than the session timeout duration, will be fully contained in that file.
 *
 * In case of failure, we close all open files. This means that files that were closed as a result of such a failure *DO NOT* provide above guarantee.
 */
public class SessionBinningFileStrategy implements FileCreateAndSyncStrategy {
    private final static Logger logger = LoggerFactory.getLogger(SessionBinningFileStrategy.class);

    private final static long HDFS_RECONNECT_DELAY = 15000;
    private final static long FILE_TIME_TO_LIVE_IN_SESSION_DURATIONS = 3;

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
    private Long failedRound;
    private long lastFixAttempt;
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
        /*
         * On setup, we assume everything to work, as we cannot open
         * any files before receiving any events. This is because the
         * events are used as a clock signal.
         */
        isHdfsAlive = true;
        failedRound = null;
        lastFixAttempt = 0;
        return SUCCESS;
    }

    @Override
    public HdfsOperationResult heartbeat() {
        if (isHdfsAlive) {
            // queue is empty, so logical time == current system time
            timeSignal = System.currentTimeMillis();
            return throwsIoException(() -> {
                    ImmutableMap.copyOf(openFiles)
                    .values()
                    .stream()
                    .distinct()
                    .forEach((f) -> {
                        throwsIoException(() -> possiblySyncAndOrClose(f))
                        .ifPresent((ioe) -> {
                            failedRound = f.round;
                            throw new WrappedIOException(ioe);
                        });
                    });
                }
            )
            .map((ioe) -> {
                logger.warn("Failed to sync HDFS file.", ioe);
                hdfsDied();
                return FAILURE;
            })
            .orElse(SUCCESS);
        } else {
            // queue may or may not be empty, just attempt a reconnect
            return possiblyFixHdfsConnection();
        }
    }

    @Override
    public HdfsOperationResult append(AvroRecordBuffer record) {
        if (!isHdfsAlive) {
            throw new IllegalStateException("Append attempt while HDFS connection is not alive.");
        }

        timeSignal = record.getEventTime();
        return writeRecord(record);
    }

    private HdfsOperationResult writeRecord(final AvroRecordBuffer record) {
        return throwsIoException(() -> {
            RoundHdfsFile file = fileForSessionStartTime(record.getSessionId().timestamp);
            file.writer.appendEncoded(record.getByteBuffer());
            file.recordsSinceLastSync += 1;
            possiblySyncAndOrClose(file);
        })
        .map((ioe) -> {
            logger.warn("Error while flushing event to HDFS.", ioe);
            failedRound = record.getSessionId().timestamp / sessionTimeoutMillis;
            hdfsDied();
            return FAILURE;
        })
        .orElse(SUCCESS);
    }

    @Override
    public void cleanup() {
        openFiles.values().forEach((file) -> {
            throwsIoException(() -> file.close())
            .ifPresent((ioe) -> {
                logger.warn("Failed to properly close HDFS file: " + file.path, ioe);
            });
        });
        openFiles.clear();
    }

    private void possiblySyncAndOrClose(RoundHdfsFile file) {
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

                possiblyCloseAndCleanup(file);
            } else if (file.recordsSinceLastSync == 0) {
                file.lastSyncTime = time;
                possiblyCloseAndCleanup(file);
            }
        } catch (IOException e) {
            throw new WrappedIOException(e);
        }
    }

    private void possiblyCloseAndCleanup(RoundHdfsFile file) {
        final long oldestAllowedRound = (timeSignal / sessionTimeoutMillis) - (FILE_TIME_TO_LIVE_IN_SESSION_DURATIONS - 1);
        if (file.round < oldestAllowedRound) {
            logger.debug("Closing HDFS file: {}", file.path);
            throwsIoException(file::close)
            .ifPresent((ioe) -> {
                logger.warn("Failed to cleanly close HDFS file: " + file.path, ioe);
            });

            for (Iterator<Entry<Long,RoundHdfsFile>> itr = openFiles.entrySet().iterator(); itr.hasNext(); ) {
                final Entry<Long, RoundHdfsFile> e = itr.next();
                if (e.getValue() == file) {
                    itr.remove();
                }
            }
        }
    }

    private HdfsOperationResult possiblyFixHdfsConnection() {
        if (isHdfsAlive) {
            throw new IllegalStateException("HDFS connection repair attempt while not broken.");
        }

        final long time = System.currentTimeMillis();
        if (time - lastFixAttempt > HDFS_RECONNECT_DELAY) {
            return throwsIoException(() -> new RoundHdfsFile(failedRound * sessionTimeoutMillis))
            .map((ioe) -> {
                logger.warn("Could not reconnect to HDFS after failure.");
                lastFixAttempt = time;
                return FAILURE;
            })
            .orElseGet(() -> {
                logger.info("Recovered HDFS connection.");
                isHdfsAlive = true;
                failedRound = null;
                lastFixAttempt = 0;
                return SUCCESS;
            });
        } else {
            return FAILURE;
        }
    }

    private void hdfsDied() {
        /*
         * On HDFS connection / access failure, we abandon everything and periodically try to reconnect,
         * by re-creating a file for the round that caused the failure. Other files will be re-created
         * as records for specific files arrive.
         */
        isHdfsAlive = false;
        openFiles.values().forEach((file) -> {
            throwsIoException(() -> file.close());
        });
        openFiles.clear();

        logger.warn("HDFS failure. Closing all files and going into connect retry cycle.");
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
            final long oldestAllowedRound = (timeSignal / sessionTimeoutMillis) - (FILE_TIME_TO_LIVE_IN_SESSION_DURATIONS - 1);
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
            /*
             * The round string in the filename is constructed from the current date
             * in the form YYYYmmdd-RR. Where RR is the 0-padded number of session length
             * intervals since midnight on the current day. This uses the system timezone.
             * Note that if the system is in a timezone that supports DST, the number of
             * session lengths intervals per day is not the same for all days.
             */
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
