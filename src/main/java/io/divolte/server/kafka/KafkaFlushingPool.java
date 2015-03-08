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
import io.divolte.server.ValidatedConfiguration;
import io.divolte.server.processing.ProcessingPool;

import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class KafkaFlushingPool extends ProcessingPool<KafkaFlusher, AvroRecordBuffer> {
    public KafkaFlushingPool(final ValidatedConfiguration vc) {
        this(
                Objects.requireNonNull(vc),
                vc.configuration().kafkaFlusher.threads,
                vc.configuration().kafkaFlusher.maxWriteQueue,
                vc.configuration().kafkaFlusher.maxEnqueueDelay.toMillis()
                );
    }

    public KafkaFlushingPool(ValidatedConfiguration vc, int numThreads, int maxWriteQueue, long maxEnqueueDelay) {
        super(numThreads, maxWriteQueue, maxEnqueueDelay, "Kafka Flusher", () -> new KafkaFlusher(vc));
    }

    public void enqueueRecord(final AvroRecordBuffer record) {
        enqueue(record.getPartyId().value, record);
    }
}
