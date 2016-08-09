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

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteIdentifier;
import io.divolte.server.processing.Item;
import io.divolte.server.processing.ItemProcessor;

@ParametersAreNonnullByDefault
@NotThreadSafe
public final class KafkaFlusher implements ItemProcessor<AvroRecordBuffer> {
    private final static Logger logger = LoggerFactory.getLogger(KafkaFlusher.class);

    private final String topic;
    private final Producer<DivolteIdentifier, AvroRecordBuffer> producer;

    // On failure, we store the list of messages that are still pending here.
    private ImmutableList<ProducerRecord<DivolteIdentifier, AvroRecordBuffer>> pendingMessages = ImmutableList.of();

    public KafkaFlusher(final String topic, final Producer<DivolteIdentifier, AvroRecordBuffer> producer) {
        this.topic = Objects.requireNonNull(topic);
        this.producer = Objects.requireNonNull(producer);
    }

    private ProducerRecord<DivolteIdentifier, AvroRecordBuffer> buildRecord(final AvroRecordBuffer record) {
        return new ProducerRecord<>(topic, record.getPartyId(), record);
    }

    @Override
    public ProcessingDirective process(final Item<AvroRecordBuffer> item) {
        final AvroRecordBuffer record = item.payload;
        logger.debug("Processing individual record: {}", record);
        return flush(ImmutableList.of(buildRecord(record)));
    }

    @Override
    public ProcessingDirective process(final Queue<Item<AvroRecordBuffer>> batch) {
        final int batchSize = batch.size();
        final ProcessingDirective result;
        switch (batchSize) {
        case 0:
            logger.warn("Ignoring empty batch of events.");
            result = CONTINUE;
            break;
        case 1:
            result = process(batch.remove());
            break;
        default:
            logger.debug("Processing batch of {} records.", batchSize);
            final List<ProducerRecord<DivolteIdentifier, AvroRecordBuffer>> kafkaMessages =
                    batch.stream()
                         .map(i -> i.payload)
                         .map(this::buildRecord)
                         .collect(Collectors.toCollection(() -> new ArrayList<>(batchSize)));
            // Clear the messages now; on failure they'll be retried as part of our
            // pending operation.
            batch.clear();
            result = flush(kafkaMessages);
        }
        return result;
    }

    @Override
    public ProcessingDirective heartbeat() {
        if (pendingMessages.isEmpty()) {
            return CONTINUE;
        } else {
            logger.debug("Retrying to send {} pending message(s) that previously failed.", pendingMessages.size());
            return flush(pendingMessages);
        }
    }

    private ProcessingDirective flush(final List<ProducerRecord<DivolteIdentifier, AvroRecordBuffer>> batch) {
        try {
            final ImmutableList<ProducerRecord<DivolteIdentifier,AvroRecordBuffer>> remaining = sendBatch(batch);
            pendingMessages = remaining;
            return remaining.isEmpty() ? CONTINUE : PAUSE;
        } catch (final InterruptedException e) {
            // This is painful. We don't know how much of the batch was stored, and how much wasn't.
            // This should only occur during shutdown. I think we can assume that anything missed can be discarded.
            logger.warn("Flushing interrupted. Not all messages in batch (size={}) may have been sent to Kafka.", batch.size());
            pendingMessages = ImmutableList.of();
            return CONTINUE;
        }
    }

    private ImmutableList<ProducerRecord<DivolteIdentifier,AvroRecordBuffer>> sendBatch(final List<ProducerRecord<DivolteIdentifier, AvroRecordBuffer>> batch) throws InterruptedException {
        // First start sending the messages.
        // (This will serialize them, determine the partition and then assign them to a per-partition buffer.)
        final int batchSize = batch.size();
        final List<Future<RecordMetadata>> sendResults =
                batch.stream()
                     .map(producer::send)
                     .collect(Collectors.toCollection(() -> new ArrayList<>(batchSize)));
        // The producer will send the messages in the background. As of 0.8.x we can't
        // flush, but have to wait for that to occur based on the producer configuration.
        // (By default it will immediately flush, but users can override this.)

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
                                 record.value(), metadata.topic(), metadata.partition(), metadata.offset());
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
                    logger.error("Error sending event (partyId=" + record.key() + ") to Kafka. Abandoning batch.", cause);
                }
            }
        }
        return remaining.build();
    }
}
