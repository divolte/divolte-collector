package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class KafkaSinkConfiguration extends SinkConfiguration {
    private static final String DEFAULT_TOPIC = "divolte";

    public final String topic;

    @JsonCreator
    KafkaSinkConfiguration(final Optional<String> topic) {
        this.topic = topic.orElse(DEFAULT_TOPIC);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper().add("topic", topic);
    }
}
