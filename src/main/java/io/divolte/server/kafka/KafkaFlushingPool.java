/*
 * Copyright 2014 GoDataDriven B.V.
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
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.processing.ProcessingPool;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import javax.annotation.ParametersAreNonnullByDefault;

import java.util.Objects;

@ParametersAreNonnullByDefault
public class KafkaFlushingPool extends ProcessingPool<KafkaFlusher, AvroRecordBuffer> {

    private final Producer<DivolteIdentifier, AvroRecordBuffer> producer;

    public KafkaFlushingPool(final ValidatedConfiguration vc) {
        this(
                vc.configuration().kafkaFlusher.threads,
                vc.configuration().kafkaFlusher.maxWriteQueue,
                vc.configuration().kafkaFlusher.maxEnqueueDelay.toMillis(),
                vc.configuration().kafkaFlusher.topic,
                new KafkaProducer<>(
                        vc.configuration().kafkaFlusher.producer,
                        vc.configuration().kafkaFlusher.producer.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) ? null : new DivolteIdentifierSerializer(),
                        vc.configuration().kafkaFlusher.producer.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) ? null : new AvroRecordBufferSerializer())
                );
    }

    public KafkaFlushingPool(final int numThreads,
                             final int maxWriteQueue,
                             final long maxEnqueueDelay,
                             final String topic,
                             final Producer<DivolteIdentifier, AvroRecordBuffer> producer ) {
        super(numThreads, maxWriteQueue, maxEnqueueDelay, "Kafka Flusher", () -> new KafkaFlusher(topic, producer));
        this.producer = Objects.requireNonNull(producer);
    }

    @Override
    public void stop() {
        super.stop();
        producer.close();
    }
}
