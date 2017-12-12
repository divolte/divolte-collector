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
import com.google.api.gax.retrying.RetrySettings;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;
import java.time.Duration;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class GoogleRetryConfiguration {
    /*
     * We leave Google's code to deal with most defaults, except for the situations where
     * they fail out-of-the-box. In these cases we provide alternative defaults. These are:
     *
     * Internal defaults from Google's builder:
     *  - TotalTimeout = 0s
     *  - InitialRetryDelay = 0s
     *  - RetryDelayMultiplier = 1.0
     *  - MaxRetryDelay = 0s
     *  - MaxAttempts = 0
     *  - Jittered = true
     *  - InitialRpcTimeout = 0s
     *  - RpcTimeoutMultiplier = 1.0
     *  - MaxRpcTimeout = 0s
     *
     * Constraints from Google's builder:
     *  - TotalTimeout >= 0s
     *  - InitialRetryDelay >= 0s
     *  - RetryDelayMultiplier >= 1.0
     *  - InitialRetryDelay <= MaxRetryDelay
     *  - MaxAttempts >= 0
     *  - InitialRpcTimeout >= 0s
     *  - InitialRpcTimeout <= MaxRpcTimeout
     *  - RpcTimeoutMultiplier >= 1.0
     *
     * Constraints from Google's PubSub:
     *  - TotalTimeout >= 10s
     *  - InitalRpcTimeout >= 10ms
     *
     * Our defaults, overriding the ones from Google:
     *  - TotalTimeout = 28 days
     *  - InitalRpcTimeout = 15s
     *  - MaxRpcTimeout = InitialRpcTimeout
     */

    private static final Duration DEFAULT_TOTAL_TIMEOUT = Duration.ofDays(28);
    private static final Duration DEFAULT_INITIAL_RPC_TIMEOUT = Duration.ofSeconds(15);

    public final Optional<Integer> maxAttempts;
    public final Duration totalTimeout;
    public final Optional<Duration> initialRetryDelay;
    public final Optional<Double> retryDelayMultiplier;
    public final Optional<Duration> maxRetryDelay;
    public final Duration initialRpcTimeout;
    public final Optional<Double> rpcTimeoutMultiplier;
    public final Duration maxRpcTimeout;

    @JsonCreator
    @ParametersAreNullableByDefault
    GoogleRetryConfiguration(final Integer maxAttempts,
                             final Duration totalTimeout,
                             final Duration initialRetryDelay,
                             final Double retryDelayMultiplier,
                             final Duration maxRetryDelay,
                             final Duration initialRpcTimeout,
                             final Double rpcTimeoutMultiplier,
                             final Duration maxRpcTimeout) {
        this.maxAttempts = Optional.ofNullable(maxAttempts);
        this.totalTimeout = Optional.ofNullable(totalTimeout).orElse(DEFAULT_TOTAL_TIMEOUT);
        this.initialRetryDelay = Optional.ofNullable(initialRetryDelay);
        this.retryDelayMultiplier = Optional.ofNullable(retryDelayMultiplier);
        this.maxRetryDelay = Optional.ofNullable(maxRetryDelay);
        this.initialRpcTimeout = Optional.ofNullable(initialRpcTimeout).orElse(DEFAULT_INITIAL_RPC_TIMEOUT);
        this.rpcTimeoutMultiplier = Optional.ofNullable(rpcTimeoutMultiplier);
        this.maxRpcTimeout = Optional.ofNullable(maxRpcTimeout).orElse(this.initialRpcTimeout);
    }

    public RetrySettings createRetrySettings() {
        RetrySettings.Builder builder = RetrySettings.newBuilder()
            .setTotalTimeout(to310bp(totalTimeout))
            .setInitialRpcTimeout(to310bp(initialRpcTimeout))
            .setMaxRpcTimeout(to310bp(maxRpcTimeout));
        maxAttempts.ifPresent(builder::setMaxAttempts);
        initialRetryDelay.map(GoogleRetryConfiguration::to310bp).ifPresent(builder::setInitialRetryDelay);
        retryDelayMultiplier.ifPresent(builder::setRetryDelayMultiplier);
        maxRetryDelay.map(GoogleRetryConfiguration::to310bp).ifPresent(builder::setMaxRetryDelay);
        rpcTimeoutMultiplier.ifPresent(builder::setRpcTimeoutMultiplier);
        return builder.build();
    }

    private static org.threeten.bp.Duration to310bp(final Duration duration) {
        return org.threeten.bp.Duration.ofSeconds(duration.getSeconds(), duration.getNano());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("maxAttempts", maxAttempts)
            .add("totalTimeout", totalTimeout)
            .add("initialRetryDelay", initialRetryDelay)
            .add("retryDelayMultiplier", retryDelayMultiplier)
            .add("maxRetryDelay", maxRetryDelay)
            .add("initialRpcTimeout", initialRpcTimeout)
            .add("rpcTimeoutMultiplier", rpcTimeoutMultiplier)
            .add("maxRpcTimeout", maxRpcTimeout)
            .toString();
    }
}
