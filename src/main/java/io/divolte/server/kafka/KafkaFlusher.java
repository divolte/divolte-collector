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
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.ValidatedConfiguration;
import io.divolte.server.processing.ItemProcessor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ParametersAreNonnullByDefault
@NotThreadSafe
public final class KafkaFlusher implements ItemProcessor<AvroRecordBuffer> {
    private final static Logger logger = LoggerFactory.getLogger(KafkaFlusher.class);

    private final String topic;
    private final Producer<byte[], byte[]> producer;

    // On failure, we pause delivery and store the failed operation here.
    // During heartbeats it will be retried until success.
    private Optional<KafkaSender> pendingOperation = Optional.empty();

    public KafkaFlusher(final ValidatedConfiguration vc) {
        Objects.requireNonNull(vc);
        final ProducerConfig producerConfig = new ProducerConfig(vc.configuration().kafkaFlusher.producer);
        topic = vc.configuration().kafkaFlusher.topic;
        producer = new Producer<>(producerConfig);
    }

    @Override
    public ProcessingDirective process(final AvroRecordBuffer record) {
        logger.debug("Processing individual record.", record);
        return send(() -> {
            producer.send(buildMessage(record));
            logger.debug("Sent individual record to Kafka.", record);
        });
    }

    @Override
    public ProcessingDirective process(final Queue<AvroRecordBuffer> batch) {
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
            final List<KeyedMessage<byte[], byte[]>> kafkaMessages =
                    batch.stream()
                         .map(this::buildMessage)
                         .collect(Collectors.toCollection(() -> new ArrayList<>(batchSize)));
            // Clear the messages now; on failure they'll be retried as part of our
            // pending operation.
            batch.clear();
            result = send(() -> {
                producer.send(kafkaMessages);
                logger.debug("Sent {} records to Kafka.", batchSize);
            });
         }
        return result;
    }

    @Override
    public ProcessingDirective heartbeat() {
        return pendingOperation.map((t) -> {
            logger.debug("Retrying to send message(s) that failed.");
            return send(t);
        }).orElse(CONTINUE);
    }

    @FunctionalInterface
    private interface KafkaSender {
        void send() throws FailedToSendMessageException;
    }

    private ProcessingDirective send(final KafkaSender sender) {
        ProcessingDirective result;
        try {
            sender.send();
            pendingOperation = Optional.empty();
            result = CONTINUE;
        } catch (final FailedToSendMessageException e) {
            logger.warn("Failed to send message(s) to Kafka! (Will retry.)", e);
            pendingOperation = Optional.of(sender);
            result = PAUSE;
        }
        return result;
    }

    private KeyedMessage<byte[], byte[]> buildMessage(final AvroRecordBuffer record) {
        // Extract the AVRO record as a byte array.
        // (There's no way to do this without copying the array.)
        final ByteBuffer avroBuffer = record.getByteBuffer();
        final byte[] avroBytes = new byte[avroBuffer.remaining()];
        avroBuffer.get(avroBytes);
        final String partyId = record.getPartyId().value;
        return new KeyedMessage<>(topic, partyId.getBytes(StandardCharsets.UTF_8), partyId, avroBytes);
    }
}
