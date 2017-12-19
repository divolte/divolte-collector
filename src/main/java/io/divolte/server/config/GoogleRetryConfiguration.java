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
     * Default values and constraints are complicated due to Google code generation. As of 0.30.0-beta,
     * the following is the situation...
     *
     * Internal defaults from Google's builder:
     *  - TotalTimeout = 0s
     *  - InitialRetryDelay = 0s
     *  - RetryDelayMultiplier = 1.0
     *  - MaxRetryDelay = 0s
     *  - MaxAttempts = 0 (infinite)
     *  - Jittered = true (unused)
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
     * Google's PubSub overrides the defaults with the following:
     *  - TotalTimeout = 10s
     *  - InitialRetryDelay = 5ms
     *  - RetryDelayMultiplier = 2
     *  - MaxRetryDelay = LONG_MAX
     *  - InitialRpcTimeout = 10s
     *  - RpcTimeoutMultiplier = 2
     *  - MaxRpcTimeout = 10s
     *
     * To override these, they must _all_ be overridden and the defaults effectively revert
     * to those above. To help a bit, we normalize things by copying the Google PubSub values
     * as defaults with the following exceptions:
     *  - TotalTimeout = 28 days
     *  - MaxRetryDelay = 1min
     *  - MaxRpcTimeout = InitialRpcTimeout
     *
     * Selective overrides are allowed.
     */

    private static final int DEFAULT_MAX_ATTEMPTS = 0;
    private static final Duration DEFAULT_TOTAL_TIMEOUT = Duration.ofDays(28);
    private static final Duration DEFAULT_INITIAL_RETRY_DELAY = Duration.ofMillis(5);
    private static final double DEFAULT_RETRY_DELAY_MULTIPLIER = 2.0;
    private static final Duration DEFAULT_MAX_RETRY_DELAY = Duration.ofMinutes(1);
    private static final Duration DEFAULT_INITIAL_RPC_TIMEOUT = Duration.ofSeconds(15);
    private static final double DEFAULT_RPC_TIMEOUT_MULTIPLIER = 2.0;

    public final int maxAttempts;
    public final Duration totalTimeout;
    public final Duration initialRetryDelay;
    public final double retryDelayMultiplier;
    public final Duration maxRetryDelay;
    public final Duration initialRpcTimeout;
    public final double rpcTimeoutMultiplier;
    public final Duration maxRpcTimeout;

    @JsonCreator
    @ParametersAreNullableByDefault
    public GoogleRetryConfiguration(final Integer maxAttempts,
                                    final Duration totalTimeout,
                                    final Duration initialRetryDelay,
                                    final Double retryDelayMultiplier,
                                    final Duration maxRetryDelay,
                                    final Duration initialRpcTimeout,
                                    final Double rpcTimeoutMultiplier,
                                    final Duration maxRpcTimeout) {
        this.maxAttempts = Optional.ofNullable(maxAttempts).orElse(DEFAULT_MAX_ATTEMPTS);
        this.totalTimeout = Optional.ofNullable(totalTimeout).orElse(DEFAULT_TOTAL_TIMEOUT);
        this.initialRetryDelay = Optional.ofNullable(initialRetryDelay).orElse(DEFAULT_INITIAL_RETRY_DELAY);
        this.retryDelayMultiplier = Optional.ofNullable(retryDelayMultiplier).orElse(DEFAULT_RETRY_DELAY_MULTIPLIER);
        this.maxRetryDelay = Optional.ofNullable(maxRetryDelay).orElse(DEFAULT_MAX_RETRY_DELAY);
        this.initialRpcTimeout = Optional.ofNullable(initialRpcTimeout).orElse(DEFAULT_INITIAL_RPC_TIMEOUT);
        this.rpcTimeoutMultiplier = Optional.ofNullable(rpcTimeoutMultiplier).orElse(DEFAULT_RPC_TIMEOUT_MULTIPLIER);
        this.maxRpcTimeout = Optional.ofNullable(maxRpcTimeout).orElse(this.initialRpcTimeout);
    }

    public RetrySettings createRetrySettings() {
        return RetrySettings.newBuilder()
                            .setMaxAttempts(maxAttempts)
                            .setTotalTimeout(to310bp(totalTimeout))
                            .setInitialRetryDelay(to310bp(initialRetryDelay))
                            .setRetryDelayMultiplier(retryDelayMultiplier)
                            .setMaxRetryDelay(to310bp(maxRetryDelay))
                            .setInitialRpcTimeout(to310bp(initialRpcTimeout))
                            .setRpcTimeoutMultiplier(rpcTimeoutMultiplier)
                            .setMaxRpcTimeout(to310bp(maxRpcTimeout)).build();
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
