package io.divolte.server.config;

import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import io.divolte.server.kafka.KafkaFlushingPool;

@ParametersAreNonnullByDefault
public class KafkaSinkConfiguration extends SinkConfiguration {
    private static final String DEFAULT_TOPIC = "divolte";
    private static final KafkaSinkMode DEFAULT_SINK_MODE = KafkaSinkMode.NAKED;

    public final String topic;
    public final KafkaSinkMode mode;

    @JsonCreator
    @ParametersAreNullableByDefault
    KafkaSinkConfiguration(@JsonProperty(defaultValue=DEFAULT_TOPIC) final String topic, @JsonProperty final KafkaSinkMode mode) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        this.topic = Optional.ofNullable(topic).orElse(DEFAULT_TOPIC);
        this.mode = Optional.ofNullable(mode).orElse(DEFAULT_SINK_MODE);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper().add("topic", topic);
    }

    @Override
    public SinkFactory getFactory() {
        return KafkaFlushingPool::createPool;
    }
}
