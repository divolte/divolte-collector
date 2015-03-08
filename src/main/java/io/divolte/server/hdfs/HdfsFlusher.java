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
import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.ValidatedConfiguration;
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

@ParametersAreNonnullByDefault
@NotThreadSafe
public final class HdfsFlusher implements ItemProcessor<AvroRecordBuffer> {
    private final static Logger logger = LoggerFactory.getLogger(HdfsFlusher.class);

    private final FileCreateAndSyncStrategy fileStrategy;
    private HdfsOperationResult lastHdfsResult;

    public HdfsFlusher(final ValidatedConfiguration vc, final Schema schema) {
        Objects.requireNonNull(vc);

        final FileSystem hadoopFs;
        final Configuration hdfsConfiguration = new Configuration();
        final short hdfsReplication = (short) vc.configuration().hdfsFlusher.hdfs.replication;

        /*
         * The HDFS client creates a JVM shutdown hook, which interferes with our own server shutdown hook.
         * This config option disabled the built in shutdown hook. We call FileSystem.closeAll() ourselves
         * in the server shutdown hook instead.
         */
        hdfsConfiguration.setBoolean("fs.automatic.close", false);
        try {
            hadoopFs = vc.configuration().hdfsFlusher.hdfs.uri.map(uri -> {
                try {
                    return FileSystem.get(new URI(uri), hdfsConfiguration);
                } catch (IOException | URISyntaxException e) {
                    /*
                     * It is possible to create a FileSystem instance when HDFS is not available (e.g. NameNode down).
                     * This exception only occurs when there is a configuration error in the URI (e.g. wrong scheme).
                     * So we fail to start up in this case. Below we create the actual HDFS connection, by opening
                     * files. If that fails, we do startup and initiate the regular retry cycle.
                     */
                    logger.error("Could not initialize HDFS filesystem.", e);
                    throw new RuntimeException("Could not initialize HDFS filesystem", e);
                }
            }).orElse(FileSystem.get(hdfsConfiguration));
        } catch (IOException ioe) {
            /*
             * It is possible to create a FileSystem instance when HDFS is not available (e.g. NameNode down).
             * This exception only occurs when there is a configuration error in the URI (e.g. wrong scheme).
             * So we fail to start up in this case. Below we create the actual HDFS connection, by opening
             * files. If that fails, we do startup and initiate the regular retry cycle.
             */
            logger.error("Could not initialize HDFS filesystem.", ioe);
            throw new RuntimeException("Could not initialize HDFS filesystem", ioe);
        }

        fileStrategy = FileCreateAndSyncStrategy.create(vc, hadoopFs, hdfsReplication, Objects.requireNonNull(schema));
        lastHdfsResult = fileStrategy.setup();
    }

    @Override
    public void cleanup() {
        fileStrategy.cleanup();
    }

    @Override
    public ProcessingDirective process(AvroRecordBuffer record) {
        if (lastHdfsResult == SUCCESS) {
            return (lastHdfsResult = fileStrategy.append(record)) == SUCCESS ? CONTINUE : PAUSE;
        } else {
            return PAUSE;
        }
    }

    @Override
    public ProcessingDirective heartbeat() {
        lastHdfsResult = fileStrategy.heartbeat();
        return lastHdfsResult == SUCCESS ? CONTINUE : PAUSE;
    }
}
