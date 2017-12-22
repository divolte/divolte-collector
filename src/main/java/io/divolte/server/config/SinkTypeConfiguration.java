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

import com.google.common.base.MoreObjects;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public abstract class SinkTypeConfiguration {

    public final boolean enabled;
    public final int bufferSize;
    public final int threads;

    protected SinkTypeConfiguration(final int bufferSize, final int threads, final boolean enabled) {
        this.bufferSize = bufferSize;
        this.threads = threads;
        this.enabled = enabled;
    }

    @OverridingMethodsMustInvokeSuper
    protected MoreObjects.ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this)
                .add("enabled", enabled)
                .add("bufferSize", bufferSize)
                .add("threads", threads);
    }

    @Override
    public final String toString() {
        return toStringHelper().toString();
    }
}
