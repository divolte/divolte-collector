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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteSchema;
import io.divolte.server.kafka.Serializers;
import org.apache.kafka.common.serialization.Serializer;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;
import java.util.function.Function;

@ParametersAreNonnullByDefault
public enum KafkaSinkMode {
    @JsonProperty("naked")
    NAKED(Serializers::createNakedAvroSerializer),
    @JsonProperty("confluent")
    CONFLUENT(Serializers::createConfluentAvroSerializer);

    final Function<DivolteSchema, Serializer<AvroRecordBuffer>> serializerFactory;

    KafkaSinkMode(Function<DivolteSchema, Serializer<AvroRecordBuffer>> serializerFactory) {
        this.serializerFactory = Objects.requireNonNull(serializerFactory);
    }
}
