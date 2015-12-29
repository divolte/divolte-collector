package io.divolte.server.config;

import javax.annotation.ParametersAreNonnullByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.divolte.server.kafka.KafkaFlushingPool;

@ParametersAreNonnullByDefault
public class KafkaSinkConfiguration extends SinkConfiguration {
    private static final String DEFAULT_TOPIC = "divolte";

    public final String topic;

    @JsonCreator
    KafkaSinkConfiguration(@JsonProperty(defaultValue=DEFAULT_TOPIC) final String topic) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue proprty from the annotation to fix this
        this.topic = topic == null ? DEFAULT_TOPIC : topic;
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper().add("topic", topic);
    }

    @Override
    public SinkFactory getFactory() {
        return KafkaFlushingPool::new;
    }
}
