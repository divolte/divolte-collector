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

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.ValidatedConfiguration;
import io.divolte.server.ValidatedConfiguration.FileStrategyConfiguration.Types;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;

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

    static FileCreateAndSyncStrategy create(final ValidatedConfiguration vc, final FileSystem fs, final short hdfsReplication, final Schema schema) {
        if (vc.configuration().hdfsFlusher.fileStrategy.type == Types.SESSION_BINNING) {
            return new SessionBinningFileStrategy(vc, fs, hdfsReplication, schema);
        } else if (vc.configuration().hdfsFlusher.fileStrategy.type == Types.SIMPLE_ROLLING_FILE) {
            return new SimpleRollingFileStrategy(vc, fs, hdfsReplication, schema);
        } else {
            // Should not occur with a validate configuration.
            throw new RuntimeException("No valid HDFS file flushing strategy was configured.");
        }
    }

    enum HdfsOperationResult {
        SUCCESS,
        FAILURE
    }
}
