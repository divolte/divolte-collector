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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

import io.divolte.server.config.HdfsSinkConfiguration;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.config.FileStrategyConfiguration;
import io.divolte.server.config.ValidatedConfiguration;

@NotThreadSafe
@ParametersAreNonnullByDefault
public class SimpleRollingFileStrategy implements FileCreateAndSyncStrategy {
    private static final Logger logger = LoggerFactory.getLogger(SimpleRollingFileStrategy.class);

    private final static long HDFS_RECONNECT_DELAY = 15000;

    private final static String INFLIGHT_EXTENSION = ".partial";

    private final static AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    private final int instanceNumber;
    private final String hostString;
    private final DateFormat datePartFormat = new SimpleDateFormat("yyyyLLddHHmmss");

    private final Schema schema;

    private final long syncEveryMillis;
    private final int syncEveryRecords;
    private final long newFileEveryMillis;

    private final FileSystem hdfs;
    private final String hdfsWorkingDir;
    private final String hdfsPublishDir;
    private final short hdfsReplication;

    private HadoopFile currentFile;
    private boolean isHdfsAlive;
    private long lastFixAttempt;

    public SimpleRollingFileStrategy(final ValidatedConfiguration vc,
                                     final String name,
                                     final FileSystem fs,
                                     final short hdfsReplication,
                                     final Schema schema) {
        Objects.requireNonNull(vc);
        this.schema = Objects.requireNonNull(schema);

        final FileStrategyConfiguration fileStrategyConfiguration =
                vc.configuration().getSinkConfiguration(name, HdfsSinkConfiguration.class).fileStrategy;
        syncEveryMillis = fileStrategyConfiguration.syncFileAfterDuration.toMillis();
        syncEveryRecords = fileStrategyConfiguration.syncFileAfterRecords;
        newFileEveryMillis = fileStrategyConfiguration.rollEvery.toMillis();

        instanceNumber = INSTANCE_COUNTER.incrementAndGet();
        hostString = findLocalHostName();

        this.hdfs = fs;
        this.hdfsReplication = hdfsReplication;

        hdfsWorkingDir = fileStrategyConfiguration.workingDir;
        hdfsPublishDir = fileStrategyConfiguration.publishDir;

        throwsIoException(() -> {
            if (!hdfs.isDirectory(new Path(hdfsWorkingDir))) {
                throw new IOException("Working directory for in-flight AVRO records does not exist: " + hdfsWorkingDir);
            }
            if (!hdfs.isDirectory(new Path(hdfsPublishDir))) {
                throw new IOException("Working directory for publishing AVRO records does not exist: " + hdfsPublishDir);
            }
        }).ifPresent((e) -> { throw new RuntimeException("Configuration error", e); });
    }

    private Path newFilePath() {
        return new Path(hdfsWorkingDir, String.format("%s-divolte-tracking-%s-%d.avro" + INFLIGHT_EXTENSION, datePartFormat.format(new Date()), hostString, instanceNumber));
    }

    private static String findLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            return "localhost";
        }
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
            throwsIoException(() -> hdfs.delete(newFilePath, false));
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

            currentFile.totalRecords += currentFile.recordsSinceLastSync;
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
            } catch (final IOException e) {
                throwsIoException(() -> hdfs.delete(newFilePath, false));
                throw e;
            }
            logger.debug("Rolling file. Opened: {}", currentFile.path);
        }
    }

    private HdfsOperationResult possiblyFixHdfsConnection() {
        if (isHdfsAlive) {
            throw new IllegalStateException("HDFS connection repair attempt while not broken.");
        }

        final long time = System.currentTimeMillis();
        if (time - lastFixAttempt > HDFS_RECONNECT_DELAY) {
            final Path newFilePath = newFilePath();
            return throwsIoException(() ->
                currentFile = openNewFile(newFilePath)
            ).map((ioe) -> {
                isHdfsAlive = false;
                lastFixAttempt = time;
                // possibly we created the file, so silently attempt a delete
                throwsIoException(() -> hdfs.delete(newFilePath, false));
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
        long totalRecords;

        public HadoopFile(final Path path) throws IOException {
            this.path = path;
            this.stream = hdfs.create(path, hdfsReplication);

            writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema)).create(schema, stream);
            writer.setSyncInterval(1 << 30);
            writer.setFlushOnEveryBlock(true);

            // Sync the file on open to make sure the
            // connection actually works, because
            // HDFS allows file creation even with no
            // datanodes available
            this.stream.hsync();

            this.openTime = this.lastSyncTime = System.currentTimeMillis();
            this.recordsSinceLastSync = 0;
            this.totalRecords = 0;
            this.projectedCloseTime = openTime + newFileEveryMillis;
        }

        private Path getPublishDestination() {
            final String pathName = path.getName();
            return new Path(hdfsPublishDir, pathName.substring(0, pathName.length() - INFLIGHT_EXTENSION.length()));
        }

        @Override
        public void close() throws IOException {
            totalRecords += recordsSinceLastSync;
            writer.close();

            if (totalRecords > 0) {
                // Publish file to destination directory
                final Path publishDestination = getPublishDestination();
                logger.debug("Moving HDFS file: {} -> {}", path, publishDestination);
                if (!hdfs.rename(path, publishDestination)) {
                    throw new IOException("Could not rename HDFS file: " + path + " -> " + publishDestination);
                }
            } else {
                // Discard empty file
                logger.debug("Deleting empty HDFS file: {}", path);
                throwsIoException(() -> hdfs.delete(path, false))
                .ifPresent((ioe) -> logger.warn("Could not delete empty HDFS file.", ioe));
            }
        }
    }

    @FunctionalInterface
    private interface IOExceptionThrower {
        void run() throws IOException;
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
