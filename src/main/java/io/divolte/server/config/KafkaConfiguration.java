package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;
import java.util.Properties;

@ParametersAreNonnullByDefault
public class KafkaConfiguration extends SinkTypeConfiguration {

    private final Properties producer;

    @JsonCreator
    KafkaConfiguration(final int bufferSize, final int threads, final boolean enabled, final Properties producer) {
        super(bufferSize, threads, enabled);
        // Defensive copy: ensure our copy remains immutable.
        this.producer = Objects.requireNonNull((Properties) producer.clone());
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("producer", producer);
    }

    public Properties getProducer() {
        // Defensive copy: we can't stop callers from modifying what we return.
        return (Properties)producer.clone();
    }
}
