package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

@ParametersAreNonnullByDefault
public final class KafkaFlusherConfiguration {
    public final boolean enabled;
    public final int threads;
    public final int maxWriteQueue;
    public final Duration maxEnqueueDelay;
    public final String topic;
    public final Properties producer;

    @JsonCreator
    private KafkaFlusherConfiguration(
            final boolean enabled,
            final int threads,
            final int maxWriteQueue,
            final Duration maxEnqueueDelay,
            final String topic,
            final Properties producer) {
        this.enabled = enabled;
        this.threads = threads;
        this.maxWriteQueue = maxWriteQueue;
        this.maxEnqueueDelay = Objects.requireNonNull(maxEnqueueDelay);
        this.topic = Objects.requireNonNull(topic);
        this.producer = Objects.requireNonNull(producer);
    }

    @Override
    public String toString() {
        return "KafkaFlusherConfiguration [enabled=" + enabled + ", threads=" + threads + ", maxWriteQueue=" + maxWriteQueue + ", maxEnqueueDelay=" + maxEnqueueDelay + ", topic=" + topic + ", producer=" + producer + "]";
    }
}
