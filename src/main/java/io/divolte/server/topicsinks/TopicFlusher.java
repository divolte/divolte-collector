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

package io.divolte.server.topicsinks;

import com.google.common.collect.ImmutableList;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteSchema;
import io.divolte.server.processing.Item;
import io.divolte.server.processing.ItemProcessor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.CONTINUE;
import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.PAUSE;

@ParametersAreNonnullByDefault
@NotThreadSafe
public abstract class TopicFlusher<T> implements ItemProcessor<AvroRecordBuffer> {
    private final static Logger logger = LoggerFactory.getLogger(TopicFlusher.class);

    protected final static String MESSAGE_ATTRIBUTE_SCHEMA_FINGERPRINT = "schemaFingerprint";

    protected static byte[] schemaFingerprint(final DivolteSchema schema) {
        final Schema avroSchema = schema.avroSchema;
        final byte[] fingerprint;
        // SHA-256 is on the list of mandatory JCE algorithms, so this shouldn't be an issue.
        try {
            fingerprint = SchemaNormalization.parsingFingerprint("SHA-256", avroSchema);
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("Cannot calculate schema fingerprint; missing SHA-256 digest algorithm", e);
        }
        return fingerprint;
    }

    // On failure, we store the list of messages that are still pending here.
    private ImmutableList<T> pendingMessages = ImmutableList.of();

    @Override
    public final ProcessingDirective process(final Item<AvroRecordBuffer> item) {
        final AvroRecordBuffer record = item.payload;
        logger.debug("Processing individual event: {}", record);
        return flush(ImmutableList.of(buildRecord(record)));
    }

    @Override
    public final ProcessingDirective process(final Queue<Item<AvroRecordBuffer>> batch) {
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
            logger.debug("Processing batch of {} events.", batchSize);
            final List<T> messages =
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
    public final ProcessingDirective heartbeat() {
        if (pendingMessages.isEmpty()) {
            return CONTINUE;
        } else {
            logger.debug("Trying to re-send {} pending event(s) that previously failed.", pendingMessages.size());
            return flush(pendingMessages);
        }
    }

    private ProcessingDirective flush(final List<T> batch) {
        try {
            final ImmutableList<T> remaining = sendBatch(batch);
            pendingMessages = remaining;
            return remaining.isEmpty() ? CONTINUE : PAUSE;
        } catch (final InterruptedException e) {
            // This is painful; we don't know how much of the batch was published and how much wasn't.
            // This should only occur during shutdown.
            logger.warn("Flushing interrupted. Not all events in batch (size={}) may have been flushed.", batch.size());
            // Preserve thread interruption invariant.
            Thread.currentThread().interrupt();
            return CONTINUE;
        }
    }

    protected abstract T buildRecord(final AvroRecordBuffer record);
    protected abstract ImmutableList<T> sendBatch(final List<T> batch) throws InterruptedException;
}
