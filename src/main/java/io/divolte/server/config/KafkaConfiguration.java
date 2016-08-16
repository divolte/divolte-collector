package io.divolte.server.config;

import java.util.Properties;

import javax.annotation.ParametersAreNonnullByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

@ParametersAreNonnullByDefault
public class KafkaConfiguration extends SinkTypeConfiguration {

    public final Properties producer;

    @JsonCreator
    KafkaConfiguration(final int bufferSize, final int threads, final boolean enabled, final Properties producer) {
        super(bufferSize, threads, enabled);
        this.producer = ImmutableProperties.fromSource(producer);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("producer", producer);
    }
}
