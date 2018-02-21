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

import com.google.common.base.MoreObjects;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Duration;
import java.util.Objects;

@ParametersAreNonnullByDefault
public abstract class RetryConfiguration {

    protected final int maxAttempts;
    protected final Duration totalTimeout;
    protected final Duration initialRetryDelay;
    protected final double retryDelayMultiplier;
    protected final Duration maxRetryDelay;

    protected RetryConfiguration(final int maxAttempts,
                                 final Duration totalTimeout,
                                 final Duration initialRetryDelay,
                                 final double retryDelayMultiplier,
                                 final Duration maxRetryDelay) {
        this.maxAttempts = maxAttempts;
        this.totalTimeout = Objects.requireNonNull(totalTimeout);
        this.initialRetryDelay = Objects.requireNonNull(initialRetryDelay);
        this.retryDelayMultiplier = retryDelayMultiplier;
        this.maxRetryDelay = Objects.requireNonNull(maxRetryDelay);
    }

    @OverridingMethodsMustInvokeSuper
    protected MoreObjects.ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this)
            .add("maxAttempts", maxAttempts)
            .add("totalTimeout", totalTimeout)
            .add("initialRetryDelay", initialRetryDelay)
            .add("retryDelayMultiplier", retryDelayMultiplier)
            .add("maxRetryDelay", maxRetryDelay);
    }

    @Override
    public final String toString() {
        return toStringHelper().toString();
    }
}
