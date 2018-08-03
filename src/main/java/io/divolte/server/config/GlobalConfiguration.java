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
import com.google.common.collect.ImmutableMap;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

@ParametersAreNonnullByDefault
public class GlobalConfiguration {
    @Valid
    public final ServerConfiguration server;
    @Valid
    public final MapperConfiguration mapper;

    @Valid
    private final ImmutableMap<String,? extends SinkTypeConfiguration> sinkTypeConfigurations;

    private final ImmutableMap<Class<? extends SinkTypeConfiguration>, ? extends SinkTypeConfiguration> sinkTypeConfigurationsByClass;

    @JsonCreator
    GlobalConfiguration(final ServerConfiguration server,
                        final MapperConfiguration mapper,
                        final HdfsConfiguration hdfs,
                        final KafkaConfiguration kafka,
                        final GoogleCloudStorageConfiguration gcs,
                        final GoogleCloudPubSubConfiguration gcps) {
        this.server = Objects.requireNonNull(server);
        this.mapper = Objects.requireNonNull(mapper);
        this.sinkTypeConfigurations = ImmutableMap.of(
            "hdfs", Objects.requireNonNull(hdfs),
            "kafka", Objects.requireNonNull(kafka),
            "gcs", Objects.requireNonNull(gcs),
            "gcps", Objects.requireNonNull(gcps)
        );
        sinkTypeConfigurationsByClass =
            sinkTypeConfigurations.values()
                                  .stream()
                                  .collect(ImmutableMap.toImmutableMap(SinkTypeConfiguration::getClass, Function.identity()));
    }

    public <T extends SinkTypeConfiguration> T getSinkTypeConfiguration(final Class<T> configurationClass) {
        return Optional.ofNullable(sinkTypeConfigurationsByClass.get(configurationClass))
                       .map(configurationClass::cast)
                       .orElseThrow(IllegalArgumentException::new);
    }

    @Override
    public String toString() {
        final MoreObjects.ToStringHelper stringHelper = MoreObjects.toStringHelper(this)
            .add("server", server)
            .add("mapper", mapper);
        sinkTypeConfigurations.forEach(stringHelper::add);
        return stringHelper.toString();
    }
}
