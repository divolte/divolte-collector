/*
 * Copyright 2016 GoDataDriven B.V.
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

package io.divolte.server.kafka;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteIdentifier;
import io.divolte.server.SchemaRegistry;
import io.divolte.server.config.KafkaConfiguration;
import io.divolte.server.config.KafkaSinkConfiguration;
import io.divolte.server.config.SinkConfiguration;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.processing.ProcessingPool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;

import javax.annotation.ParametersAreNonnullByDefault;

public class KafkaFlushingPoolFactory implements SinkConfiguration.SinkFactory {

    @Override
    @ParametersAreNonnullByDefault
    public ProcessingPool<?, AvroRecordBuffer> create(ValidatedConfiguration vc, String name, SchemaRegistry schemaRegistry) {
        KafkaSinkConfiguration sinkConfiguration = vc.configuration().getSinkConfiguration(name, KafkaSinkConfiguration.class);
        KafkaConfiguration kafkaConfiguration = vc.configuration().global.kafka;
        KafkaProducer<DivolteIdentifier, AvroRecordBuffer> producer =
            new KafkaProducer<>(kafkaConfiguration.producer,
                createKeySerializer(sinkConfiguration, kafkaConfiguration),
                createValueSerializer(sinkConfiguration, name, schemaRegistry)
            );
        return new KafkaFlushingPool(name,
            kafkaConfiguration.threads,
            kafkaConfiguration.bufferSize,
            sinkConfiguration.topic,
            producer
        );
    }

    private Serializer<DivolteIdentifier> createKeySerializer(KafkaSinkConfiguration sinkConfiguration, KafkaConfiguration kafkaConfiguration) {
        switch (sinkConfiguration.mode) {
            case CONFLUENT:
                return new ConfluentDivolteIdentifierSerializer(
                    kafkaConfiguration.confluentKeyId.get()
                );
            default:
                return new DivolteIdentifierSerializer();
        }
    }

    private Serializer<AvroRecordBuffer> createValueSerializer(KafkaSinkConfiguration sinkConfiguration, String name, SchemaRegistry schemaRegistry) {
        switch (sinkConfiguration.mode) {
            case CONFLUENT:
                return new ConfluentAvroRecordBufferSerializer(
                    schemaRegistry.getSchemaBySinkName(name).confluentId.get()
                );
            default:
                return new AvroRecordBufferSerializer();
        }
    }
}
