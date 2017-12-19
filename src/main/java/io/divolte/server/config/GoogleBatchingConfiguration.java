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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.api.gax.batching.BatchingSettings;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;
import java.time.Duration;
import java.util.Optional;

import static io.divolte.server.config.GoogleCloudPubSubSinkConfiguration.to310bp;

@ParametersAreNonnullByDefault
public class GoogleBatchingConfiguration {
    /*
     * Default values and contraints are complicated due to Google code generation. As of 0.30.0-beta,
     * the following is the situation...
     *
     * Internal defaults from Google's builder:
     *  - IsEnabled = true (unused)
     *  - ElementCountThreshold = 1
     *  - RequestByteThreshold = 1
     *  - DelayThreshold = 1ms
     *
     * Constraints from Google's builder:
     *  - ElementCountThreshold = null or > 0
     *  - RequestByteThreshold = null or > 0
     *  - DelayThreshold = null or > 0
     *
     * Constraints from Google's PubSub:
     *  - ElementCountThreshold >= 1
     *  - RequestByteThreshold >= 1
     *  - DelayThreshold >= 1ms
     *
     * Google's PubSub uses its own set of defaults:
     *  - ElementCountThreshold = 100
     *  - RequestBytesThreshold = 1 kB
     *  - DelayThreshold = 1ms
     *
     * When electing to override the PubSub default settings, all values must be set and the internal
     * defaults from above apply. To help a bit, we normalize things by copying the Google PubSub values
     * as defaults. This effectively allows selective overriding without affecting other values.
     */
    private static final long DEFAULT_ELEMENT_COUNT_THRESHOLD = 100L;
    private static final long DEFAULT_REQUEST_BYTES_THRESHOLD = 1000L;
    private static final Duration DEFAULT_DELAY_THRESHOLD = Duration.ofMillis(1);

    public final long elementCountThreshold;
    public final long requestBytesThreshold;
    public final Duration delayThreshold;

    @JsonCreator
    @ParametersAreNullableByDefault
    public GoogleBatchingConfiguration(final Long elementCountThreshold,
                                       final Long requestBytesThreshold,
                                       final Duration delayThreshold) {
        this.elementCountThreshold = Optional.ofNullable(elementCountThreshold).orElse(DEFAULT_ELEMENT_COUNT_THRESHOLD);
        this.requestBytesThreshold = Optional.ofNullable(requestBytesThreshold).orElse(DEFAULT_REQUEST_BYTES_THRESHOLD);
        this.delayThreshold = Optional.ofNullable(delayThreshold).orElse(DEFAULT_DELAY_THRESHOLD);
    }

    public BatchingSettings createBatchingSettings() {
        return BatchingSettings.newBuilder()
                               .setElementCountThreshold(elementCountThreshold)
                               .setRequestByteThreshold(requestBytesThreshold)
                               .setDelayThreshold(to310bp(delayThreshold))
                               .build();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("elementCountThreshold", elementCountThreshold)
            .add("requestBytesThreshold", requestBytesThreshold)
            .add("delayThreshold", delayThreshold)
            .toString();
    }
}
