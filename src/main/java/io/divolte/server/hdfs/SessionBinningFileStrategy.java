/*
 * Copyright 2014 GoDataDriven B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.divolte.server.hdfs;

import static io.divolte.server.hdfs.FileCreateAndSyncStrategy.HdfsOperationResult.*;
import static java.util.Calendar.*;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.ValidatedConfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

/*
 * The general idea of this file strategy is to provide a best effort to put events that belong to the same session in the same file.
 *
 * The session binning file strategy assigns event to files as such:
 * - each timestamp is assigned to a round, defined as timestamp_in_millis / session_timeout_in_millis
 * - we open a file for a round as time passes
 * - all events for a session are stored in the file with the round marked by the session start time
 * - a file for a round is kept open for at least three times the session duration *in absence of failures*
 * - during this entire process, we use the event timestamp for events that come off the queue as a logical clock signal
 *      - only in the case of an empty queue, we use the actual system time as clock signal (receiving heartbeats in a state of normal operation means an empty queue)
 * - when a file for a round is closed, but events that should be in that file still arrive, they are stored in the oldest open file
 *      - this happens for exceptionally long sessions
 *
 * The above mechanics allow for the following guarantee: if a file is properly opened, used for flushing and closed without intermediate failures,
 * all sessions that start within that file and last less than the session timeout duration, will be fully contained in that file.
 *
 * In case of failure, we close all open files. This means that files that were closed as a result of such a failure *DO NOT* provide above guarantee.
 */
@NotThreadSafe
public class SessionBinningFileStrategy implements FileCreateAndSyncStrategy {
    private final static Logger logger = LoggerFactory.getLogger(SessionBinningFileStrategy.class);

    private final static long HDFS_RECONNECT_DELAY_MILLIS = 15000;
    private final static long FILE_TIME_TO_LIVE_IN_SESSION_DURATIONS = 3;

    private final static AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    private final int instanceNumber;
    private final String hostString;


    private final FileSystem hdfs;
    private final short hdfsReplication;

    private final Schema schema;

    private final long sessionTimeoutMillis;

    private final Map<Long, RoundHdfsFile> openFiles;
    private final String hdfsWorkingDir;
    private final String hdfsPublishDir;
    private final long syncEveryMillis;
    private final int syncEveryRecords;

    private boolean isHdfsAlive;
    private long lastFixAttempt;
    private long timeSignal;

    private long lastSyncTime;
    private int recordsSinceLastSync;


    public SessionBinningFileStrategy(final ValidatedConfiguration vc, final FileSystem hdfs, final short hdfsReplication, final Schema schema) {
        sessionTimeoutMillis = vc.configuration().tracking.sessionTimeout.toMillis();

        hostString = findLocalHostName();
        instanceNumber = INSTANCE_COUNTER.incrementAndGet();
        hdfsWorkingDir = vc.configuration().hdfsFlusher.fileStrategy.asSessionBinningFileStrategy().workingDir;
        hdfsPublishDir = vc.configuration().hdfsFlusher.fileStrategy.asSessionBinningFileStrategy().publishDir;

        syncEveryMillis = vc.configuration().hdfsFlusher.fileStrategy.asSessionBinningFileStrategy().syncFileAfterDuration.toMillis();
        syncEveryRecords = vc.configuration().hdfsFlusher.fileStrategy.asSessionBinningFileStrategy().syncFileAfterRecords;

        this.hdfs = hdfs;
        this.hdfsReplication = hdfsReplication;

        this.schema = schema;

        openFiles = Maps.newHashMapWithExpectedSize(10);

        throwsIoException(() -> {
            if (!hdfs.isDirectory(new Path(hdfsWorkingDir))) {
                throw new IOException("Working directory for in-flight AVRO records does not exist: " + hdfsWorkingDir);
            }
            if (!hdfs.isDirectory(new Path(hdfsPublishDir))) {
                throw new IOException("Working directory for publishing AVRO records does not exist: " + hdfsPublishDir);
            }
        }).ifPresent((e) -> { throw new RuntimeException("Configuration error", e); });
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
        lastFixAttempt = 0;

        lastSyncTime = 0;
        recordsSinceLastSync = 0;

        return SUCCESS;
    }

    @Override
    public HdfsOperationResult heartbeat() {
        if (isHdfsAlive) {
            // queue is empty, so logical time == current system time
            timeSignal = System.currentTimeMillis();
            return throwsIoException(this::possiblySyncAndOrClose)
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
            final RoundHdfsFile file = fileForSessionStartTime(record.getSessionId().timestamp - record.getCookieUtcOffset());
            file.writer.appendEncoded(record.getByteBuffer());
            file.recordsSinceLastSync += 1;
            recordsSinceLastSync += 1;
            possiblySyncAndOrClose();
        })
        .map((ioe) -> {
            logger.warn("Error while flushing event to HDFS.", ioe);
            hdfsDied();
            return FAILURE;
        })
        .orElse(SUCCESS);
    }

    @Override
    public void cleanup() {
        openFiles.values().forEach((file) -> throwsIoException(() -> file.close(false))
        .ifPresent((ioe) -> logger.warn("Failed to properly close HDFS file: " + file.path, ioe)));
        openFiles.clear();
    }

    private void possiblySyncAndOrClose() {
        try {
            final long time = System.currentTimeMillis();

            if (
                    recordsSinceLastSync >= syncEveryRecords ||
                    time - lastSyncTime >= syncEveryMillis && recordsSinceLastSync > 0) {

                openFiles
                .values()
                .stream()
                .filter((f) -> f.recordsSinceLastSync > 0) // only sync files that have pending records
                .forEach((file) -> {
                    try {
                        logger.debug("Syncing file: {}", file.path);
                        file.writer.sync();   // Forces the Avro file to write a block
                        file.stream.hsync();  // Forces a (HDFS) sync on the underlying stream
                        file.recordsSinceLastSync = 0;
                    } catch (IOException e) {
                        throw new WrappedIOException(e);
                    }
                });

                recordsSinceLastSync = 0;
                lastSyncTime = time;
            } else if (recordsSinceLastSync == 0) {
                lastSyncTime = time;
            }
        } finally {
            possiblyCloseAndCleanup();
        }
    }

    private void possiblyCloseAndCleanup() {
        final long oldestAllowedRound = (timeSignal / sessionTimeoutMillis) - (FILE_TIME_TO_LIVE_IN_SESSION_DURATIONS - 1);

        List<Entry<Long, RoundHdfsFile>> entriesToBeClosed = openFiles
        .entrySet()
        .stream()
        .filter((e) -> e.getValue().round < oldestAllowedRound)
        .collect(Collectors.toList());

        entriesToBeClosed
        .stream()
        .map(Entry::getValue)
        .distinct()
        .forEach((file) -> {
            logger.debug("Closing HDFS file: {}", file.path);
            throwsIoException(() -> file.close(true))
            .ifPresent((ioe) -> logger.warn("Failed to cleanly close HDFS file: " + file.path, ioe));
        });

        entriesToBeClosed
        .forEach((e) -> openFiles.remove(e.getKey()));
    }

    private HdfsOperationResult possiblyFixHdfsConnection() {
        if (isHdfsAlive) {
            throw new IllegalStateException("HDFS connection repair attempt while not broken.");
        }

        final long time = System.currentTimeMillis();
        if (time - lastFixAttempt > HDFS_RECONNECT_DELAY_MILLIS) {
            return throwsIoException(() -> openFiles.put(timeSignal / sessionTimeoutMillis, new RoundHdfsFile(timeSignal)))
            .map((ioe) -> {
                logger.warn("Could not reconnect to HDFS after failure.");
                lastFixAttempt = time;
                return FAILURE;
            })
            .orElseGet(() -> {
                logger.info("Recovered HDFS connection.");
                isHdfsAlive = true;
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
        openFiles.values().forEach((file) -> throwsIoException(() -> file.close(false)));
        openFiles.clear();

        logger.warn("HDFS failure. Closing all files and going into connect retry cycle.");
    }

    private RoundHdfsFile fileForSessionStartTime(final long sessionStartTime) {
        final long requestedRound = sessionStartTime / sessionTimeoutMillis;
        // return the first open file for which the round >= the requested round
        // or create a new file if no such file is present
        return openFiles.computeIfAbsent(requestedRound, (ignored) -> openFiles
            .values()
            .stream()
            .sorted((left, right) -> Long.compare(left.round, right.round))
            .filter((f) -> f.round >= requestedRound)
            .findFirst()
            .orElseGet(() ->
                // if the requested round is greater than the current round + 1,
                // we return the file for the current round, as probably this is
                // a result of a very skewed client side clock, or a fake request
                requestedRound > timeSignal / sessionTimeoutMillis + 1
                    ? fileForSessionStartTime(timeSignal)
                    : new RoundHdfsFile(sessionStartTime)
            ));
    }

    private final class RoundHdfsFile {
        private static final String INFLIGHT_EXTENSION = ".partial";
        private static final int MAX_AVRO_SYNC_INTERVAL = 1 << 30;
        private final DateFormat format = new SimpleDateFormat("HH.mm.ss.SSS");

        final Path path;
        final long round;
        final FSDataOutputStream stream;
        final DataFileWriter<GenericRecord> writer;

        int recordsSinceLastSync;

        RoundHdfsFile(final long time) {
            final long requestedRound = time / sessionTimeoutMillis;
            final long oldestAllowedRound = (timeSignal / sessionTimeoutMillis) - (FILE_TIME_TO_LIVE_IN_SESSION_DURATIONS - 1);
            this.round = Math.max(requestedRound, oldestAllowedRound);

            this.path = new Path(hdfsWorkingDir,
                    String.format("%s-divolte-tracking-%s-%s-%d.avro" + INFLIGHT_EXTENSION,
                            hostString, // add host name, differentiates when deploying multiple collector instances
                            roundString(round * sessionTimeoutMillis), // composed of the round start date + round number within the day
                            format.format(new Date()), // additionally, we add a timestamp, because after failures, a file for a round can be created multiple times
                            instanceNumber)); // add instance number, so different threads cannot try to create the exact same file

            try {
                stream = hdfs.create(path, hdfsReplication);
                writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema)).create(schema, stream);
                writer.setSyncInterval(MAX_AVRO_SYNC_INTERVAL); // since we manually sync at chosen intervals
                writer.setFlushOnEveryBlock(true);

                // Sync the file on open to make sure the
                // connection actually works, because
                // HDFS allows file creation even with no
                // datanodes available
                stream.hsync();
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
             * session length intervals per day is not equal for all days.
             */
            final GregorianCalendar gc = new GregorianCalendar();
            gc.setTimeInMillis(roundStartTime);
            gc.set(HOUR_OF_DAY, 0);
            gc.set(MINUTE, 0);
            gc.set(SECOND, 0);
            gc.set(MILLISECOND, 0);

            return String.format("%d%02d%02d-%02d",
                    gc.get(YEAR),
                    gc.get(MONTH) + 1,
                    gc.get(DAY_OF_MONTH),
                    (roundStartTime - gc.getTimeInMillis()) / sessionTimeoutMillis);
        }

        private Path getPublishDestination() {
            final String pathName = path.getName();
            return new Path(hdfsPublishDir, pathName.substring(0, pathName.length() - INFLIGHT_EXTENSION.length()));
        }

        public void close(final boolean publish) {
            try {
                writer.close();
                if (publish) {
                    final Path publishDestination = getPublishDestination();
                    logger.debug("Moving HDFS file: {} -> {}", path, publishDestination);
                    if (!hdfs.rename(path, publishDestination)) {
                        throw new IOException("Could not rename HDFS file: " + path + " -> " + publishDestination);
                    }
                }
            } catch (IOException e) {
                throw new WrappedIOException(e);
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class WrappedIOException extends RuntimeException {
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
