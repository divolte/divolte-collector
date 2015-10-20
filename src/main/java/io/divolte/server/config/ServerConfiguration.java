package io.divolte.server.config;

import java.util.Objects;

import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@ParametersAreNullableByDefault
public final class ServerConfiguration {

    public final String host;
    public final int port;
    public final boolean useXForwardedFor;
    public final boolean serveStaticResources;

    @JsonCreator
    private ServerConfiguration(final String host, final Integer port, @JsonProperty("use_x_forwarded_for") final Boolean useXForwardedFor, final Boolean serveStaticResources) {
        this.host = host;
        this.port = port;
        this.useXForwardedFor = useXForwardedFor;
        this.serveStaticResources = Objects.requireNonNull(serveStaticResources, "Cannot be null.");
    }

    @Override
    public String toString() {
        return "ServerConfiguration [host=" + host + ", port=" + port + ", useXForwardedFor=" + useXForwardedFor + ", serveStaticResources=" + serveStaticResources + "]";
    }
}
