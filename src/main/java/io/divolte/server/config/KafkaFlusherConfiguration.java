package io.divolte.server.config;

import java.time.Duration;
import java.util.Properties;

import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;

@ParametersAreNullableByDefault
public final class KafkaFlusherConfiguration {
    public final Boolean enabled;
    public final Integer threads;
    public final Integer maxWriteQueue;
    public final Duration maxEnqueueDelay;
    public final String topic;
    public final Properties producer;

    @JsonCreator
    private KafkaFlusherConfiguration(
            final Boolean enabled,
            final Integer threads,
            final Integer maxWriteQueue,
            final Duration maxEnqueueDelay,
            final String topic,
            final Properties producer) {
        this.enabled = enabled;
        this.threads = threads;
        this.maxWriteQueue = maxWriteQueue;
        this.maxEnqueueDelay = maxEnqueueDelay;
        this.topic = topic;
        this.producer = producer;
    }

    @Override
    public String toString() {
        return "KafkaFlusherConfiguration [enabled=" + enabled + ", threads=" + threads + ", maxWriteQueue=" + maxWriteQueue + ", maxEnqueueDelay=" + maxEnqueueDelay + ", topic=" + topic + ", producer=" + producer + "]";
    }
}
