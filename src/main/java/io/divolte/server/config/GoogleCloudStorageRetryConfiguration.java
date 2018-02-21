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
import com.google.common.base.MoreObjects;
import io.divolte.server.config.constraint.EitherJitterDurationOrFactorButNotBoth;
import net.jodah.failsafe.RetryPolicy;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@ParametersAreNonnullByDefault
@EitherJitterDurationOrFactorButNotBoth
public class GoogleCloudStorageRetryConfiguration extends RetryConfiguration {
    private static final int DEFAULT_MAX_ATTEMPTS = 0;
    private static final Duration DEFAULT_TOTAL_TIMEOUT = Duration.ofMinutes(10);
    private static final Duration DEFAULT_INITIAL_RETRY_DELAY = Duration.ofMillis(5);
    private static final double DEFAULT_RETRY_DELAY_MULTIPLIER = 2.0;
    private static final Duration DEFAULT_MAX_RETRY_DELAY = Duration.ofMinutes(1);
    private static final Optional<Double> DEFAULT_JITTER_FACTOR = Optional.of(0.25);

    // Either of these may be set, but never both.
    public final Optional<Double> jitterFactor;
    public final Optional<Duration> jitterDelay;

    @JsonCreator
    @ParametersAreNullableByDefault
    public GoogleCloudStorageRetryConfiguration(final Integer maxAttempts,
                                                final Duration totalTimeout,
                                                final Duration initialRetryDelay,
                                                final Double retryDelayMultiplier,
                                                final Duration maxRetryDelay,
                                                final Double jitterFactor,
                                                final Duration jitterDelay) {
        super(Optional.ofNullable(maxAttempts).orElse(DEFAULT_MAX_ATTEMPTS),
              Optional.ofNullable(totalTimeout).orElse(DEFAULT_TOTAL_TIMEOUT),
              Optional.ofNullable(initialRetryDelay).orElse(DEFAULT_INITIAL_RETRY_DELAY),
              Optional.ofNullable(retryDelayMultiplier).orElse(DEFAULT_RETRY_DELAY_MULTIPLIER),
              Optional.ofNullable(maxRetryDelay).orElse(DEFAULT_MAX_RETRY_DELAY));
        this.jitterDelay = Optional.ofNullable(jitterDelay);
        // The default jitter factor only applies if neither a duration nor a factor were specified.
        this.jitterFactor = null != jitterFactor
            ? Optional.of(jitterFactor)
            : this.jitterDelay.isPresent() ? Optional.empty() : DEFAULT_JITTER_FACTOR;
    }

    public RetryPolicy createRetryPolicy() {
        final RetryPolicy policyBeforeJitter = new RetryPolicy()
            .withMaxRetries(maxAttempts - 1)
            .withMaxDuration(totalTimeout.toNanos(), TimeUnit.NANOSECONDS)
            .withBackoff(initialRetryDelay.toNanos(), maxRetryDelay.toNanos(), TimeUnit.NANOSECONDS, retryDelayMultiplier);
        final RetryPolicy policyWithJitterFactor = jitterFactor.map(policyBeforeJitter::withJitter).orElse(policyBeforeJitter);
        return jitterDelay.map(d -> policyWithJitterFactor.withJitter(d.toNanos(), TimeUnit.NANOSECONDS)).orElse(policyWithJitterFactor);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
            .add("jitterFactor", jitterFactor)
            .add("jitterDelay", jitterDelay);
    }
}
