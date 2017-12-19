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

package io.divolte.server.topicsinks.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteSchema;
import io.divolte.server.processing.Item;
import io.divolte.server.processing.ItemProcessor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.CONTINUE;
import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.PAUSE;

@ParametersAreNonnullByDefault
public final class GoogleCloudPubSubFlusher implements ItemProcessor<AvroRecordBuffer> {
    private final static Logger logger = LoggerFactory.getLogger(GoogleCloudPubSubFlusher.class);
    private final static String MESSAGE_ATTRIBUTE_PARTYID = "partyIdentifier";
    private final static String MESSAGE_ATTRIBUTE_SCHEMA_CONFLUENT_ID = "schemaConfluentId";
    private final static String MESSAGE_ATTRIBUTE_SCHEMA_FINGERPRINT = "schemaFingerprint";

    // The most compact fingerprint encoding that is practical is Base64, using the URL encoding
    // because that's safe for file names (which some registries might use to index schemas).
    private final static Base64.Encoder FINGERPRINT_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private final Publisher publisher;
    private final String schemaFingerprint;
    private final Optional<String> schemaConfluentId;

    // On failure, we store the list of messages that are still pending here.
    private ImmutableList<PubsubMessage> pendingMessages = ImmutableList.of();

    public GoogleCloudPubSubFlusher(final Publisher publisher,
                                    final DivolteSchema schema) {
        this.publisher = Objects.requireNonNull(publisher);
        this.schemaFingerprint = schemaFingerprint(schema);
        this.schemaConfluentId = schema.confluentId.map(i -> "0x" + Integer.toHexString(i));
    }

    private static String schemaFingerprint(final DivolteSchema schema) {
        final Schema avroSchema = schema.avroSchema;
        final byte[] fingerprint;
        // SHA-256 is on the list of mandatory JCE algorithms, so this shouldn't be an issue.
        try {
            fingerprint = SchemaNormalization.parsingFingerprint("SHA-256", avroSchema);
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("Cannot calculate schema fingerprint; missing SHA-256 digest algorithm", e);
        }
        return FINGERPRINT_ENCODER.encodeToString(fingerprint);
    }

    private PubsubMessage buildRecord(final AvroRecordBuffer record) {
        final PubsubMessage.Builder builder = PubsubMessage.newBuilder()
            .putAttributes(MESSAGE_ATTRIBUTE_SCHEMA_FINGERPRINT, schemaFingerprint)
            .putAttributes(MESSAGE_ATTRIBUTE_PARTYID, record.getPartyId().toString())
            .setData(ByteString.copyFrom(record.getByteBuffer()));
        return schemaConfluentId
            .map(id -> builder.putAttributes(MESSAGE_ATTRIBUTE_SCHEMA_CONFLUENT_ID, id))
            .orElse(builder)
            .build();
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
            final List<PubsubMessage> messages =
                    batch.stream()
                         .map(i -> i.payload)
                         .map(this::buildRecord)
                         .collect(Collectors.toCollection(() -> new ArrayList<>(batchSize)));
            // Clear the messages now; on failure they'll be retried as part of our
            // pending operation.
            batch.clear();
            result = flush(messages);
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

    private ProcessingDirective flush(final List<PubsubMessage> batch) {
        try {
            final ImmutableList<PubsubMessage> remaining = sendBatch(batch);
            pendingMessages = remaining;
            return remaining.isEmpty() ? CONTINUE : PAUSE;
        } catch (final InterruptedException e) {
            // This is painful; we don't know how much of the batch was published and how much wasn't.
            // This should only occur during shutdown.
            logger.warn("Flushing interrupted. Not all messages in batch (size={}) may have been published.", batch.size());
            // Preserve thread interruption invariant.
            Thread.currentThread().interrupt();
            return CONTINUE;
        }
    }

    private ImmutableList<PubsubMessage> sendBatch(final List<PubsubMessage> batch) throws InterruptedException {
        // For Pub/Sub we assume the following:
        //  - Batching behaviour is set to flush everything ASAP.
        //  - Retry behaviour will retry indefinitely, so long as it seems likely to succeed.

        // First start sending the messages.
        // (This will serialize them, determine the partition and then assign them to a per-partition buffer.)
        final int batchSize = batch.size();
        final List<ApiFuture<String>> sendResults =
                batch.stream()
                     .map(publisher::publish)
                     .collect(Collectors.toCollection(() -> new ArrayList<>(batchSize)));

        // At this point the messages are in flight, and we assume being flushed.
        // When they eventually complete, each message can be in one of several states:
        //  - Completed.
        //  - An error occurred, but a retry may succeed.
        //  - A fatal error occurred.
        final ImmutableList.Builder<PubsubMessage> remaining = ImmutableList.builder();
        for (int i = 0; i < batchSize; ++i) {
            final ApiFuture<String> pendingResult = sendResults.get(i);
            try {
                final String messageId = pendingResult.get();
                if (logger.isDebugEnabled()) {
                    final PubsubMessage message = batch.get(i);
                    logger.debug("Finished sending event (partyId={}) to Pub/Sub: messageId = {}",
                                 message.getAttributesOrThrow(MESSAGE_ATTRIBUTE_PARTYID), messageId);
                }
            } catch (final ExecutionException e) {
                final PubsubMessage message = batch.get(i);
                // The Pub/Sub publisher internally has a retry policy, but outside that we also
                // retry indefinitely unless it's a cause that we don't understand.
                final Throwable cause = e.getCause();
                if (cause instanceof ApiException) {
                    final ApiException apiException = (ApiException)cause;
                    if (apiException.isRetryable()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Transient error sending event (partyId=" + message.getAttributesOrThrow(MESSAGE_ATTRIBUTE_PARTYID) + ") to Pub/Sub; retrying.", cause);
                        }
                        remaining.add(message);
                    } else {
                        logger.warn("Permanent error sending event (partyId=" + message.getAttributesOrThrow(MESSAGE_ATTRIBUTE_PARTYID) + ") to Pub/Sub; abandoning.", cause);
                    }
                } else {
                    logger.error("Unknown error sending event (partyId=" + message.getAttributesOrThrow(MESSAGE_ATTRIBUTE_PARTYID) + ") to Pub/Sub; abandoning.", cause);
                }
            }
        }
        return remaining.build();
    }
}
