/*
 * Copyright 2017 GoDataDriven B.V.
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

import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import io.divolte.server.filesinks.FileFlushingPool;
import io.divolte.server.filesinks.FileManager.FileManagerFactory;
import io.divolte.server.filesinks.gcs.GoogleCloudStorageFileManager;
import org.apache.avro.Schema;

@ParametersAreNonnullByDefault
public class GoogleCloudStorageSinkConfiguration extends FileSinkConfiguration {
    public final String bucket;

    @JsonCreator
    GoogleCloudStorageSinkConfiguration(final FileStrategyConfiguration fileStrategy, @JsonProperty(required = true) final String bucket) {
        super(fileStrategy);
        this.bucket = Objects.requireNonNull(bucket);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
            .add("bucket", bucket);
    }

    @Override
    public SinkFactory getFactory() {
        return (config, name, registry) -> {
            final Schema schema = registry.getSchemaBySinkName(name).schema;
            final FileManagerFactory fileManagerFactory = GoogleCloudStorageFileManager.newFactory(config, name, schema);
            fileManagerFactory.verifyFileSystemConfiguration();
            return new FileFlushingPool(config, name, fileManagerFactory);
        };
    }

    @Override
    public String getReadableType() {
        return "Google Cloud Storage";
    }
}
