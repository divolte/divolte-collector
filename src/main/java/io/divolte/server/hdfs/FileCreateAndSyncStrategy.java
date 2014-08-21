package io.divolte.server.hdfs;

import io.divolte.server.AvroRecordBuffer;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;

import com.typesafe.config.Config;

/*
 * Used by the HdfsFlusher to actually flush events to HDFS. Different implementation
 * can use different strategies for assigning events to files and opening and closing files.
 *
 * Clients are expected to use append(...) whenever events are available and periodically call
 * heartbeat() when no events are available. When either append(...) or heartbeat return FAILURE,
 * clients MUST NOT call append(...) any more, until a call to heartbeat() has returned SUCCESS.
 */
interface FileCreateAndSyncStrategy {
    HdfsOperationResult setup();
    HdfsOperationResult heartbeat();
    HdfsOperationResult append(final AvroRecordBuffer record);
    void cleanup();

    public static FileCreateAndSyncStrategy create(final Config config, final FileSystem fs, final short hdfsReplication, final Schema schema) {
        if (config.hasPath("divolte.hdfs_flusher.simple_rolling_file_strategy")) {
            return new SimpleRollingFileStrategy(config, fs, hdfsReplication, schema);
        } else {
            throw new RuntimeException("No valid HDFS file flushing strategy was configured.");
        }
    }

    enum HdfsOperationResult {
        SUCCESS,
        FAILURE
    }
}
