package io.divolte.server.hdfs;

import static io.divolte.server.hdfs.HdfsFileUtils.*;
import static java.util.Calendar.*;
import io.divolte.server.AvroRecordBuffer;

import java.io.IOException;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;

public class SessionBinningFileStrategy implements FileCreateAndSyncStrategy {
    private final static AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    private final int instanceNumber;
    private final String hostString;

    private final Deque<RoundHdfsFile> openFiles;
    private final Map<Integer,RoundHdfsFile> roundAllocation;

    private final FileSystem hdfs;
    private final short hdfsReplication;

    private final Schema schema;

    private final long sessionTimeoutMinutes;

    public SessionBinningFileStrategy(final Config config, final FileSystem hdfs, final short hdfsReplication, final Schema schema) {
        sessionTimeoutMinutes = config.getDuration("divolte.tracking.session_timeout", TimeUnit.MINUTES);

        hostString = findLocalHostName();
        instanceNumber = INSTANCE_COUNTER.incrementAndGet();

        openFiles = null;
        roundAllocation = null;

        this.hdfs = hdfs;
        this.hdfsReplication = hdfsReplication;

        this.schema = schema;
    }

    @Override
    public HdfsOperationResult setup() {
        return null;
    }

    @Override
    public HdfsOperationResult heartbeat() {
        return null;
    }

    @Override
    public HdfsOperationResult append(AvroRecordBuffer record) {
        return null;
    }

    @Override
    public void cleanup() {
    }

    private void setupFile() {
        final GregorianCalendar gc = new GregorianCalendar();

        final String round = String.format("%d%02d%02d-%02d",
                gc.get(YEAR),
                gc.get(MONTH),
                gc.get(DAY_OF_MONTH),
                (gc.get(HOUR_OF_DAY) * 60 + gc.get(MINUTE)) / sessionTimeoutMinutes);
    }

    private final class RoundHdfsFile implements AutoCloseable {
        final Path path;
        final FSDataOutputStream stream;
        final DataFileWriter<GenericRecord> writer;

        long lastSyncTime;
        int recordsSinceLastSync;

        public RoundHdfsFile(Path path) throws IOException {
            this.path = path;
            this.stream = hdfs.create(path, hdfsReplication);

            this.writer = setupAvroWriter(stream, schema);

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
