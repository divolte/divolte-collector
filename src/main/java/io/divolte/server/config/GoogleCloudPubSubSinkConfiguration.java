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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.collect.Streams;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import io.divolte.server.IOExceptions;
import io.divolte.server.pubsub.GoogleCloudPubSubFlushingPool;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class GoogleCloudPubSubSinkConfiguration extends TopicSinkConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(GoogleCloudPubSubSinkConfiguration.class);

    @JsonCreator
    @ParametersAreNullableByDefault
    GoogleCloudPubSubSinkConfiguration(@JsonProperty(defaultValue=DEFAULT_TOPIC) final String topic) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        super(topic);
    }

    @Override
    public SinkFactory getFactory() {
        // Retry settings:
        //  - Unlimited attempts
        //  - For up to ~100 years.
        //    (Effectively forever, and chosen because there's no 'unlimited' value, and using Long.MAX_VALUE
        //     triggers overflow to a negative value.)
        //  - The timeout for each attempt starts at 15 seconds, doubling on each attempt up to a maximum of a minute.
        //  - The retry delay starts at 100ms, doubling on each attempt up to a max of 10s.
        //    (Note that the retry delay is not the actual delay: the actual delay is a random number between 0 and
        //     the retry delay.)
        final RetrySettings retrySettings =
            RetrySettings.newBuilder()
                         .setMaxAttempts(0)
                         .setTotalTimeout(Duration.ofDays(36524))
                         .setInitialRetryDelay(Duration.ofMillis(100))
                         .setRetryDelayMultiplier(2.0)
                         .setMaxRetryDelay(Duration.ofSeconds(10))
                         .setInitialRpcTimeout(Duration.ofSeconds(15))
                         .setRpcTimeoutMultiplier(2)
                         .setMaxRpcTimeout(Duration.ofMinutes(1))
                         .build();
        final Optional<String> emulator = Optional.ofNullable(System.getenv("PUBSUB_EMULATOR_HOST"));
        return emulator.map(hostport -> createFlushingPool(retrySettings, hostport))
                       .orElseGet(() -> createFlushingPool(retrySettings));
    }

    private SinkFactory createFlushingPool(final RetrySettings retrySettings) {
        return (vc, sinkName, registry) -> {
            final TopicName topicName = TopicName.of(vc.configuration().global.gcps.projectId, topic);
            final Publisher.Builder builder = Publisher.newBuilder(topicName).setRetrySettings(retrySettings);
            final Publisher publisher = IOExceptions.wrap(builder::build).get();
            return new GoogleCloudPubSubFlushingPool(sinkName,
                                                     vc.configuration().global.gcps.threads,
                                                     vc.configuration().global.gcps.bufferSize,
                                                     publisher,
                                                     Optional.empty(),
                                                     registry.getSchemaBySinkName(sinkName));
        };
    }

    private SinkFactory createFlushingPool(final RetrySettings retrySettings, final String hostPort) {
        // Based on Google's PubSub documentation:
        //   https://cloud.google.com/pubsub/docs/emulator#pubsub-emulator-java
        // What's going on here? Well…
        //  - Authentication is disabled; the emulator doesn't use or support it.
        //  - When Pub/Sub wants an I/O channel to talk to its Google Cloud endpoint, we're substituting our
        //    own endpoint instead. This channel also has TLS disabled, because the emulator doesn't need, use
        //    or support it.
        //
        return (vc, sinkName, registry) -> {
            logger.info("Configuring sink to use Google Cloud Pub/Sub emulator: {}", sinkName, hostPort);
            final TopicName topicName = TopicName.of(vc.configuration().global.gcps.projectId, topic);
            final ManagedChannel channel = ManagedChannelBuilder.forTarget(hostPort).usePlaintext(true).build();
            final TransportChannelProvider channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            // There's no easy way to create topics for the emulator, so we create the topic ourselves.
            createTopic(hostPort, channelProvider, topicName);
            final Publisher.Builder builder =
                Publisher.newBuilder(topicName)
                         .setRetrySettings(retrySettings)
                         .setChannelProvider(channelProvider)
                         .setCredentialsProvider(NoCredentialsProvider.create());
            final Publisher publisher = IOExceptions.wrap(builder::build).get();
            return new GoogleCloudPubSubFlushingPool(sinkName,
                                                     vc.configuration().global.gcps.threads,
                                                     vc.configuration().global.gcps.bufferSize,
                                                     publisher,
                                                     Optional.of(channel),
                                                     registry.getSchemaBySinkName(sinkName));
        };
    }

    private static void createTopic(final String hostPort,
                                    final TransportChannelProvider channelProvider,
                                    final TopicName topic) {
        final TopicAdminClient topicClient;
        try {
            final TopicAdminSettings topicAdminSettings = TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build();
            topicClient = TopicAdminClient.create(topicAdminSettings);
        } catch (final IOException e) {
            throw new UncheckedIOException(String.format("Error creating topic %s for pub/sub emulator %s",
                                                         topic, hostPort), e);
        }
        final ProjectName project = ProjectName.of(topic.getProject());
        if (Streams.stream(topicClient.listTopics(project).iterateAll())
                   .map(Topic::getNameAsTopicName)
                   .noneMatch(topic::equals)) {
            logger.info("Initializing Pub/Sub emulator topic: {}", topic);
            topicClient.createTopic(topic);
        }
    }
}
