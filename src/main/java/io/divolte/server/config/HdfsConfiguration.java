package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public final class HdfsConfiguration {
    public final Optional<String> uri;
    public final short replication;

    @JsonCreator
    private HdfsConfiguration(final Optional<String> uri, final short replication) {
        this.uri = Objects.requireNonNull(uri);
        this.replication = replication;
    }

    @Override
    public String toString() {
        return "HdfsConfiguration [uri=" + uri + ", replication=" + replication + "]";
    }
}
