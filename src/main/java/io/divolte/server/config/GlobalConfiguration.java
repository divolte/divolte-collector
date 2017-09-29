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

import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

@ParametersAreNonnullByDefault
public class GlobalConfiguration {
    @Valid public final ServerConfiguration server;
    @Valid public final MapperConfiguration mapper;
    @Valid public final HdfsConfiguration hdfs;
    @Valid public final KafkaConfiguration kafka;
    @Valid public final GoogleCloudStorageConfiguration gcs;

    @JsonCreator
    GlobalConfiguration(final ServerConfiguration server,
                        final MapperConfiguration mapper,
                        final HdfsConfiguration hdfs,
                        final KafkaConfiguration kafka,
                        final GoogleCloudStorageConfiguration gcs) {
        this.server = Objects.requireNonNull(server);
        this.mapper = Objects.requireNonNull(mapper);
        this.hdfs = Objects.requireNonNull(hdfs);
        this.kafka = Objects.requireNonNull(kafka);
        this.gcs = Objects.requireNonNull(gcs);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("server", server)
                .add("mapper", mapper)
                .add("hdfs", hdfs)
                .add("gcs", gcs)
                .add("kafka", kafka)
                .toString();
    }
}
