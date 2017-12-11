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

import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteIdentifier;
import io.divolte.server.kafka.KafkaFlushingPool;
import io.divolte.server.kafka.Serializers;
import org.apache.kafka.clients.producer.KafkaProducer;

@ParametersAreNonnullByDefault
public class KafkaSinkConfiguration extends TopicSinkConfiguration {
    private static final KafkaSinkMode DEFAULT_SINK_MODE = KafkaSinkMode.NAKED;

    public final KafkaSinkMode mode;

    @JsonCreator
    @ParametersAreNullableByDefault
    KafkaSinkConfiguration(@JsonProperty(defaultValue=DEFAULT_TOPIC) final String topic,
                           @JsonProperty final KafkaSinkMode mode) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        super(topic);
        this.mode = Optional.ofNullable(mode).orElse(DEFAULT_SINK_MODE);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
            .add("mode", mode);
    }

    @Override
    public SinkFactory getFactory() {
        return (vc, sink, registry) -> {
            final KafkaProducer<DivolteIdentifier, AvroRecordBuffer> producer =
                new KafkaProducer<>(vc.configuration().global.kafka.producer,
                                    Serializers.createKeySerializer(),
                                    mode.serializerFactory.apply(registry.getSchemaBySinkName(sink)));
            return new KafkaFlushingPool(sink,
                                         vc.configuration().global.kafka.threads,
                                         vc.configuration().global.kafka.bufferSize,
                                         topic,
                                         producer
            );
        };
    }
}
