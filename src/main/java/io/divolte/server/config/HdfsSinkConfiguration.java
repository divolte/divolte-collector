/*
 * Copyright 2018 GoDataDriven B.V.
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

package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.divolte.server.filesinks.FileFlushingPool;
import io.divolte.server.filesinks.FileManager.FileManagerFactory;
import io.divolte.server.filesinks.hdfs.HdfsFileManager;
import org.apache.avro.Schema;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class HdfsSinkConfiguration extends FileSinkConfiguration<HdfsConfiguration> {
    private static final String DEFAULT_REPLICATION = "3";

    public final short replication;

    @JsonCreator
    @ParametersAreNullableByDefault
    HdfsSinkConfiguration(@JsonProperty(defaultValue=DEFAULT_REPLICATION) final Short replication,
                          final FileStrategyConfiguration fileStrategy) {
        super(fileStrategy);
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        this.replication = Optional.ofNullable(replication).orElseGet(() -> Short.valueOf(DEFAULT_REPLICATION));
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("replication", replication);
    }

    @Override
    public SinkFactory getFactory() {
        return (config, name, registry) -> {
            final Schema avroschema = registry.getSchemaBySinkName(name).avroSchema;
            final FileManagerFactory fileManagerFactory = HdfsFileManager.newFactory(config, name, avroschema);
            fileManagerFactory.verifyFileSystemConfiguration();
            final HdfsConfiguration hdfsConfiguration = getGlobalConfiguration(config);
            return new FileFlushingPool(config, name, hdfsConfiguration.threads, hdfsConfiguration.bufferSize, fileManagerFactory);
        };
    }

    @Override
    public String getReadableType() {
        return "Hadoop FileSystem (HDFS)";
    }
}
