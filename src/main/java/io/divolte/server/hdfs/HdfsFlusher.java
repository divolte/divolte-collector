package io.divolte.server.hdfs;

import static io.divolte.server.hdfs.FileCreateAndSyncStrategy.HdfsOperationResult.*;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.hdfs.FileCreateAndSyncStrategy.HdfsOperationResult;
import io.divolte.server.processing.ItemProcessor;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

@ParametersAreNonnullByDefault
@NotThreadSafe
final class HdfsFlusher implements ItemProcessor<AvroRecordBuffer> {
    private final static Logger logger = LoggerFactory.getLogger(HdfsFlusher.class);

    private final FileSystem hadoopFs;
    private final short hdfsReplication;

    private final FileCreateAndSyncStrategy fileStrategy;
    private HdfsOperationResult lastHdfsResult;

    public HdfsFlusher(final Config config, final Schema schema) {
        Objects.requireNonNull(config);

        try {
            hdfsReplication = (short) config.getInt("divolte.hdfs_flusher.hdfs.replication");
            final URI hdfsLocation = new URI(config.getString("divolte.hdfs_flusher.hdfs.uri"));
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

        fileStrategy = FileCreateAndSyncStrategy.create(config, hadoopFs, hdfsReplication, Objects.requireNonNull(schema));
        lastHdfsResult = fileStrategy.setup();
    }

    @Override
    public void cleanup() {
        fileStrategy.cleanup();
    }

    @Override
    public void process(AvroRecordBuffer record) {
        if (lastHdfsResult == SUCCESS) {
            lastHdfsResult = fileStrategy.append(record);
        }
    }

    @Override
    public void heartbeat() {
        lastHdfsResult = fileStrategy.heartbeat();
    }
}
