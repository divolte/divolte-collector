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

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Properties;

@ParametersAreNonnullByDefault
public class KafkaConfiguration extends SinkTypeConfiguration {

    public final Properties producer;

    @JsonCreator
    KafkaConfiguration(final int bufferSize, final int threads, final boolean enabled, final Properties producer) {
        super(bufferSize, threads, enabled);
        this.producer = ImmutableProperties.fromSource(producer);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("producer", producer);
    }
}
