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

package io.divolte.server.topicsinks.kafka;

import com.google.common.collect.ImmutableList;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteIdentifier;
import io.divolte.server.topicsinks.TopicFlusher;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@ParametersAreNonnullByDefault
@NotThreadSafe
public final class KafkaFlusher extends TopicFlusher<ProducerRecord<DivolteIdentifier, AvroRecordBuffer>> {
    private final static Logger logger = LoggerFactory.getLogger(KafkaFlusher.class);

    private final String topic;
    private final Producer<DivolteIdentifier, AvroRecordBuffer> producer;

    KafkaFlusher(final String topic, final Producer<DivolteIdentifier, AvroRecordBuffer> producer) {
        this.topic = Objects.requireNonNull(topic);
        this.producer = Objects.requireNonNull(producer);
    }

    @Override
    protected ProducerRecord<DivolteIdentifier, AvroRecordBuffer> buildRecord(final AvroRecordBuffer record) {
        return new ProducerRecord<>(topic, record.getPartyId(), record);
    }

    @Override
    protected ImmutableList<ProducerRecord<DivolteIdentifier,AvroRecordBuffer>> sendBatch(final List<ProducerRecord<DivolteIdentifier, AvroRecordBuffer>> batch) throws InterruptedException {
        // First start sending the messages.
        // (This will serialize them, determine the partition and then assign them to a per-partition buffer.)
        final int batchSize = batch.size();
        final List<Future<RecordMetadata>> sendResults =
                batch.stream()
                     .map(producer::send)
                     .collect(Collectors.toCollection(() -> new ArrayList<>(batchSize)));
        // Force a flush so we can check the results without blocking unnecessarily due to
        // a user-configured flushing policy.
        producer.flush();

        // When finished, each message can be in one of several states.
        //  - Completed.
        //  - An error occurred, but a retry may succeed.
        //  - A fatal error occurred.
        // (In addition, we can be interrupted due to shutdown.)
        final ImmutableList.Builder<ProducerRecord<DivolteIdentifier, AvroRecordBuffer>> remaining = ImmutableList.builder();
        for (int i = 0; i < batchSize; ++i) {
            final Future<RecordMetadata> result = sendResults.get(i);
            try {
                final RecordMetadata metadata = result.get();
                if (logger.isDebugEnabled()) {
                    final ProducerRecord<DivolteIdentifier, AvroRecordBuffer> record = batch.get(i);
                    logger.debug("Finished sending event (partyId={}) to Kafka: topic/partition/offset = {}/{}/{}",
                                 record.key(), metadata.topic(), metadata.partition(), metadata.offset());
                }
            } catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                final ProducerRecord<DivolteIdentifier, AvroRecordBuffer> record = batch.get(i);
                if (cause instanceof RetriableException) {
                    // A retry may succeed.
                    if (logger.isDebugEnabled()) {
                        logger.debug("Transient error sending event (partyId=" + record.key() + ") to Kafka. Will retry.", cause);
                    }
                    remaining.add(record);
                } else {
                    // Fatal error.
                    logger.error("Error sending event (partyId=" + record.key() + ") to Kafka; abandoning.", cause);
                }
            }
        }
        return remaining.build();
    }
}
