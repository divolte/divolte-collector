package io.divolte.server.config;

import java.util.Optional;
import java.util.Properties;

import javax.annotation.ParametersAreNonnullByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

@ParametersAreNonnullByDefault
public final class HdfsConfiguration extends SinkTypeConfiguration {

    public final Optional<Properties> client;

    @JsonCreator
    HdfsConfiguration(final boolean enabled, final int bufferSize, final int threads, final Optional<Properties> client) {
        super(bufferSize, threads, enabled);
        this.client = client.map(ImmutableProperties::fromSource);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("client", client);
    }
}
