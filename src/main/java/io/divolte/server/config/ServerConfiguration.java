package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public final class ServerConfiguration {

    public final Optional<String> host;
    public final int port;
    public final boolean useXForwardedFor;
    public final boolean serveStaticResources;

    @JsonCreator
    ServerConfiguration(final Optional<String> host,
                        final int port,
                        @JsonProperty("use_x_forwarded_for") final boolean useXForwardedFor,
                        final boolean serveStaticResources) {
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.useXForwardedFor = useXForwardedFor;
        this.serveStaticResources = serveStaticResources;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("host", host)
                .add("port", port)
                .add("useXForwardedFor", useXForwardedFor)
                .add("serverStaticResources", serveStaticResources)
                .toString();
    }
}
