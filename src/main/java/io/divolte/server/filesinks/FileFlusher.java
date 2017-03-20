package io.divolte.server.filesinks;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.config.FileStrategyConfiguration;
import io.divolte.server.filesinks.FileManager.DivolteFile;
import io.divolte.server.processing.Item;
import io.divolte.server.processing.ItemProcessor;

public class FileFlusher implements ItemProcessor<AvroRecordBuffer> {
    private static final Logger logger = LoggerFactory.getLogger(FileFlusher.class);

    private final static long FILE_SYSTEM_RECONNECT_DELAY = 15000;

    private final static AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    private final int instanceNumber;
    private final String hostString;
    private final DateFormat datePartFormat = new SimpleDateFormat("yyyyLLddHHmmss");

    private final long syncEveryMillis;
    private final int syncEveryRecords;
    private final long newFileEveryMillis;

    private final FileManager manager;

    private TrackedFile currentFile;
    private boolean fileSystemAlive;
    private long lastFixAttempt;

	public FileFlusher(final FileStrategyConfiguration configuration, final String sinkName, final FileManager manager) {
        syncEveryMillis = configuration.syncFileAfterDuration.toMillis();
        syncEveryRecords = configuration.syncFileAfterRecords;
        newFileEveryMillis = configuration.rollEvery.toMillis();

        instanceNumber = INSTANCE_COUNTER.incrementAndGet();
        hostString = findLocalHostName();

        this.manager = manager;

        // open initial file
        // on success or failure, update fs live flag
        final String fileName = String.format("%s-divolte-tracking-%s-%d.avro", datePartFormat.format(new Date()), hostString, instanceNumber);
        try {
            currentFile = new TrackedFile(manager.createFile(fileName));
            fileSystemAlive = true;
        } catch(final IOException ioe) {
            fileSystemAlive = false;
            // Hack to force going into heartbeat / recover cycle. Potentially drops a record too many, but avoids a additional branch in process(...).
            currentFile = brokenTrackedFile(ioe);
        }
    }

    @Override
	public ProcessingDirective process(final Item<AvroRecordBuffer> item) {
        final long time = System.currentTimeMillis();
        try {
            currentFile.file.append(item.payload);
            currentFile.recordsSinceLastSync += 1;

            possiblySyncAndOrRoll(time);
        } catch(final IOException ioe) {
            logger.error("File system connection error. Marking file system as unavailable. Attempting reconnect after " + FILE_SYSTEM_RECONNECT_DELAY + " ms.", ioe);
            lastFixAttempt = time;
            fileSystemAlive = false;
            discardQuietly(currentFile);

            return PAUSE;
        }
        return CONTINUE;
	}

    @Override
    public ProcessingDirective heartbeat() {
        try {
            final long timeMillis = System.currentTimeMillis();
            if (fileSystemAlive) {
                possiblySyncAndOrRoll(timeMillis);
            } else {
                logger.info("Attempting file system reconnect after.");
                possiblyReconnectFileSystem(timeMillis);
                fileSystemAlive = true;
            }
        } catch (final IOException e) {
            logger.error("File system connection error. Marking file system as unavailable. Attempting reconnect after " + FILE_SYSTEM_RECONNECT_DELAY + " ms.", e);
            fileSystemAlive = false;
            discardQuietly(currentFile);
        }
        return fileSystemAlive ? CONTINUE : PAUSE;
    }

    @Override
    public void cleanup() {
        try {
            currentFile.file.closeAndPublish();
        } catch (final IOException ioe) {
            logger.error("Failed to close and publish file " + currentFile + " during cleanup.", ioe);
        }
    }

    private void possiblySyncAndOrRoll(final long time) throws IOException {
        if (
                currentFile.recordsSinceLastSync >= syncEveryRecords ||
                time - currentFile.lastSyncTime >= syncEveryMillis && currentFile.recordsSinceLastSync > 0) {
            logger.debug("Syncing file: {}", currentFile);

            currentFile.file.sync();
            currentFile.totalRecords += currentFile.recordsSinceLastSync;
            currentFile.recordsSinceLastSync = 0;
            currentFile.lastSyncTime = time;

            possiblyRoll(time);
        } else if (currentFile.recordsSinceLastSync == 0) {
            currentFile.lastSyncTime = time;
            possiblyRoll(time);
        }
    }

    private void possiblyRoll(final long time) throws IOException {
        if (time > currentFile.projectedCloseTime) {
            if (currentFile.totalRecords > 0) {
                logger.debug("Rolling file: {}", currentFile);
                currentFile.file.closeAndPublish();
            } else {
                logger.debug("Discarding empty file: {}", currentFile);
                currentFile.file.discard();
            }

            currentFile = new TrackedFile(manager.createFile(newFileName()));
            logger.debug("Created new file: {}", currentFile);
        }
    }

    private void possiblyReconnectFileSystem(final long time) throws IOException {
        if (time - lastFixAttempt > FILE_SYSTEM_RECONNECT_DELAY) {
            lastFixAttempt = time;
            currentFile = new TrackedFile(manager.createFile(newFileName()));
            logger.info("Recovered file system connection when creating file: {}", currentFile);
        }
    }

    private final class TrackedFile {
        final long openTime;
        final long projectedCloseTime;

        final DivolteFile file;

        long lastSyncTime;
        int recordsSinceLastSync;
        long totalRecords;


        public TrackedFile(final DivolteFile file) {
            this.file = file;

            this.openTime = this.lastSyncTime = System.currentTimeMillis();
            this.recordsSinceLastSync = 0;
            this.totalRecords = 0;
            this.projectedCloseTime = openTime + newFileEveryMillis;
        }

        @Override
        public String toString() {
            return MoreObjects
                .toStringHelper(getClass())
                .add("file", file.toString())
                .add("open time", openTime)
                .add("last sync time", lastSyncTime)
                .add("records since last sync", recordsSinceLastSync)
                .add("total records", totalRecords)
                .toString();
        }
    }

    private void discardQuietly(final TrackedFile file) {
        try {
            file.file.discard();
        } catch (final IOException e) {
            logger.warn("Failed to discard / delete file: " + file);
        }
    }

    private String newFileName() {
        return String.format("%s-divolte-tracking-%s-%d.avro", datePartFormat.format(new Date()), hostString, instanceNumber);
    }

    private static String findLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            return "localhost";
        }
    }

    private TrackedFile brokenTrackedFile(final IOException error) {
        return new TrackedFile(new DivolteFile() {
            @Override
            public void closeAndPublish() throws IOException {
                throw error;
            }

            @Override
            public void append(final AvroRecordBuffer buffer) throws IOException {
                throw error;
            }

            @Override
            public void sync() throws IOException {
                throw error;
            }

            @Override
            public void discard() throws IOException {
                throw error;
            }
        });
    }
}
