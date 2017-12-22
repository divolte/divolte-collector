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

import com.google.cloud.pubsub.v1.Publisher;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteSchema;
import io.divolte.server.processing.ProcessingPool;
import io.grpc.ManagedChannel;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class GoogleCloudPubSubFlushingPool extends ProcessingPool<GoogleCloudPubSubFlusher, AvroRecordBuffer> {

    private final Publisher publisher;
    private final Optional<ManagedChannel> channel;

    public GoogleCloudPubSubFlushingPool(final String name,
                                         final int numThreads,
                                         final int maxWriteQueue,
                                         final Publisher publisher,
                                         final Optional<ManagedChannel> channel,
                                         final DivolteSchema schema) {
        super(numThreads,
              maxWriteQueue,
              String.format("Google Cloud Pub/Sub Flusher [%s]", Objects.requireNonNull(name)),
              () -> new GoogleCloudPubSubFlusher(publisher, schema));
        this.publisher = Objects.requireNonNull(publisher);
        this.channel = Objects.requireNonNull(channel);
    }

    @Override
    public void stop() {
        super.stop();
        try {
            publisher.shutdown();
            channel.ifPresent(ManagedChannel::shutdown);
        } catch (final RuntimeException e) {
            // Pass-through without re-wrapping.
            throw e;
        } catch (final Exception e) {
            final String topicName = publisher.getTopicName().getTopic();
            throw new RuntimeException("Error shutting down pub/sub publisher for topic: " + topicName, e);
        }
    }
}
