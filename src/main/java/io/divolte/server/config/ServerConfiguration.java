package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
    private ServerConfiguration(final Optional<String> host,
                                final int port,
                                @JsonProperty("use_x_forwarded_for") final boolean useXForwardedFor,
                                final boolean serveStaticResources) {
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.useXForwardedFor = useXForwardedFor;
        this.serveStaticResources = Objects.requireNonNull(serveStaticResources, "Cannot be null.");
    }

    @Override
    public String toString() {
        return "ServerConfiguration [host=" + host + ", port=" + port + ", useXForwardedFor=" + useXForwardedFor + ", serveStaticResources=" + serveStaticResources + "]";
    }
}
