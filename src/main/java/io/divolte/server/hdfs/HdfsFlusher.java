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

import java.io.IOException;
import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

import io.divolte.server.config.HdfsSinkConfiguration;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.hdfs.FileCreateAndSyncStrategy.HdfsOperationResult;
import io.divolte.server.processing.Item;
import io.divolte.server.processing.ItemProcessor;

@ParametersAreNonnullByDefault
@NotThreadSafe
public final class HdfsFlusher implements ItemProcessor<AvroRecordBuffer> {
    private final static Logger logger = LoggerFactory.getLogger(HdfsFlusher.class);

    private final FileCreateAndSyncStrategy fileStrategy;
    private HdfsOperationResult lastHdfsResult;

    public HdfsFlusher(final ValidatedConfiguration vc, final String name, final Schema schema) {
        Objects.requireNonNull(vc);

        final Configuration hdfsConfiguration = vc.configuration().global.hdfs.client
                .map(clientProperties -> {
                    final Configuration configuration = new Configuration(false);
                    for (final String propertyName : clientProperties.stringPropertyNames()) {
                        configuration.set(propertyName, clientProperties.getProperty(propertyName));
                    }
                    return configuration;
                })
                .orElse(new Configuration());
        /*
         * The HDFS client creates a JVM shutdown hook, which interferes with our own server shutdown hook.
         * This config option disabled the built in shutdown hook. We call FileSystem.closeAll() ourselves
         * in the server shutdown hook instead.
         */
        hdfsConfiguration.setBoolean("fs.automatic.close", false);

        final FileSystem hadoopFs;
        try {
            hadoopFs = FileSystem.get(hdfsConfiguration);
        } catch (final IOException e) {
            /*
             * It is possible to create a FileSystem instance when HDFS is not available (e.g. NameNode down).
             * This exception only occurs when there is a configuration error in the URI (e.g. wrong scheme).
             * So we fail to start up in this case. Below we create the actual HDFS connection, by opening
             * files. If that fails, we do startup and initiate the regular retry cycle.
             */
            logger.error("Could not initialize HDFS filesystem.", e);
            throw new RuntimeException("Could not initialize HDFS filesystem", e);
        }
        final short hdfsReplication =
                vc.configuration()
                  .getSinkConfiguration(Objects.requireNonNull(name), HdfsSinkConfiguration.class).replication;

        fileStrategy = new SimpleRollingFileStrategy(vc, name, hadoopFs, hdfsReplication, Objects.requireNonNull(schema));
        lastHdfsResult = fileStrategy.setup();
    }

    @Override
    public void cleanup() {
        fileStrategy.cleanup();
    }

    @Override
    public ProcessingDirective process(final Item<AvroRecordBuffer> item) {
        final AvroRecordBuffer record = item.payload;
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
