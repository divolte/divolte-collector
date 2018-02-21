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

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import io.divolte.server.filesinks.FileFlushingPool;
import io.divolte.server.filesinks.FileManager.FileManagerFactory;
import io.divolte.server.filesinks.gcs.GoogleCloudStorageFileManager;
import net.jodah.failsafe.RetryPolicy;
import org.apache.avro.Schema;

public class GoogleCloudStorageSinkConfiguration extends FileSinkConfiguration {

    public final String bucket;

    /*
     * Settings in Divolte's GCS client
     *  - HttpRetryExponentialBackoff = 1s
     *  - HttpRetryExponentialBackoffMax = 256s
     *  - HttpRetryExponentialBackoffDelayFactor = 2.0
     *  - HttpRetryMaxRetries = 5
     *  - HttpRetryJitterMilliseconds = 100
     */
    private static final Duration HTTP_RETRY_EXPONENTIAL_BACKOFF = Duration.ofSeconds(1);
    private static final Duration HTTP_RETRY_EXPONENTIAL_BACKOFF_MAX = Duration.ofSeconds(256);
    private static final double HTTP_RETRY_EXPONENTIAL_BACKOFF_DELAY_FACTOR = 2.0;
    private static final int HTTP_RETRY_BACKOFF_MAX_RETRIES = -1;
    private static final int HTTP_RETRY_JITTER_MILLISECONDS = 100;

    private final Duration httpRetryExponentialBackoff;
    private final Duration httpRetryExponentialBackoffMax;
    private final double httpRetryExponentialBackoffDelayFactor;
    private final int httpRetryMaxRetries;
    private final int httpRetryJitterMilliseconds;

    @JsonCreator
    GoogleCloudStorageSinkConfiguration(final FileStrategyConfiguration fileStrategy,
                                        @JsonProperty(required = true) final String bucket,
                                        final Duration httpRetryExponentialBackoff,
                                        final Duration httpRetryExponentialBackoffMax,
                                        final Double httpRetryExponentialBackoffDelayFactor,
                                        final Integer httpRetryMaxRetries,
                                        final Integer httpRetryJitterMilliseconds) {
        super(fileStrategy);
        this.bucket = Objects.requireNonNull(bucket);

        this.httpRetryExponentialBackoff =  Optional.ofNullable(httpRetryExponentialBackoff).orElse(HTTP_RETRY_EXPONENTIAL_BACKOFF);
        this.httpRetryExponentialBackoffMax =  Optional.ofNullable(httpRetryExponentialBackoffMax).orElse(HTTP_RETRY_EXPONENTIAL_BACKOFF_MAX);
        this.httpRetryExponentialBackoffDelayFactor =  Optional.ofNullable(httpRetryExponentialBackoffDelayFactor).orElse(HTTP_RETRY_EXPONENTIAL_BACKOFF_DELAY_FACTOR);
        this.httpRetryMaxRetries =  Optional.ofNullable(httpRetryMaxRetries).orElse(HTTP_RETRY_BACKOFF_MAX_RETRIES);
        this.httpRetryJitterMilliseconds =  Optional.ofNullable(httpRetryJitterMilliseconds).orElse(HTTP_RETRY_JITTER_MILLISECONDS);
    }

    public RetryPolicy createRetryPolicy() {
        return  new RetryPolicy()
            .withBackoff(
                this.httpRetryExponentialBackoff.getSeconds(),
                this.httpRetryExponentialBackoffMax.getSeconds(),
                TimeUnit.SECONDS,
                this.httpRetryExponentialBackoffDelayFactor
            )
            .withMaxRetries(this.httpRetryMaxRetries)
            .withJitter(this.httpRetryJitterMilliseconds, TimeUnit.MILLISECONDS);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
            .add("bucket", bucket)
            .add("httpRetryExponentialBackoff", httpRetryExponentialBackoff)
            .add("httpRetryExponentialBackoffMax", httpRetryExponentialBackoffMax)
            .add("httpRetryExponentialBackoffDelayFactor", httpRetryExponentialBackoffDelayFactor)
            .add("httpRetryMaxRetries", httpRetryMaxRetries)
            .add("httpRetryJitterMilliseconds", httpRetryJitterMilliseconds);
    }

    @Override
    public SinkFactory getFactory() {
        return (config, name, registry) -> {
            final Schema avroSchema = registry.getSchemaBySinkName(name).avroSchema;
            final FileManagerFactory fileManagerFactory = GoogleCloudStorageFileManager.newFactory(config, name, avroSchema);
            fileManagerFactory.verifyFileSystemConfiguration();
            return new FileFlushingPool(config, name, fileManagerFactory);
        };
    }

    @Override
    public String getReadableType() {
        return "Google Cloud Storage";
    }
}
