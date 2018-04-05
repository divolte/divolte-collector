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
import io.divolte.server.filesinks.gcs.GoogleCloudStorageFileManager;
import org.apache.avro.Schema;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class GoogleCloudStorageSinkConfiguration extends FileSinkConfiguration {

    static final GoogleCloudStorageRetryConfiguration DEFAULT_RETRY_SETTINGS =
        new GoogleCloudStorageRetryConfiguration(null, null, null, null, null, null, null);

    public final String bucket;
    @Valid public final GoogleCloudStorageRetryConfiguration retrySettings;

    @JsonCreator
    GoogleCloudStorageSinkConfiguration(@Nullable final FileStrategyConfiguration fileStrategy,
                                        @JsonProperty(required=true) final String bucket,
                                        @Nullable final GoogleCloudStorageRetryConfiguration retrySettings) {
        super(fileStrategy);
        this.bucket = Objects.requireNonNull(bucket);
        this.retrySettings = Optional.ofNullable(retrySettings).orElse(DEFAULT_RETRY_SETTINGS);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
            .add("bucket", bucket)
            .add("retrySettings", retrySettings);
    }

    @Override
    public SinkFactory getFactory() {
        return (config, name, registry) -> {
            final Schema avroSchema = registry.getSchemaBySinkName(name).avroSchema;
            final FileManagerFactory fileManagerFactory = GoogleCloudStorageFileManager.newFactory(config, name, avroSchema);
            fileManagerFactory.verifyFileSystemConfiguration();

            final int threads = config.configuration().global.gcs.threads;
            final int bufferSize = config.configuration().global.gcs.bufferSize;

            return new FileFlushingPool(config, name, threads, bufferSize, fileManagerFactory);
        };
    }

    @Override
    public String getReadableType() {
        return "Google Cloud Storage";
    }
}
