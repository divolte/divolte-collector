package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.divolte.server.kafka.KafkaFlushingPool;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class KafkaSinkConfiguration extends SinkConfiguration {
    private static final String DEFAULT_TOPIC = "divolte";

    public final String topic;

    @JsonCreator
    @ParametersAreNullableByDefault
    KafkaSinkConfiguration(@JsonProperty(defaultValue=DEFAULT_TOPIC) final String topic) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        this.topic = Optional.ofNullable(topic).orElse(DEFAULT_TOPIC);
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
