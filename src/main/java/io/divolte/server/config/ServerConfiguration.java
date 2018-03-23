/*
 * Copyright 2018 GoDataDriven B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public final class ServerConfiguration {

    public final Optional<String> host;
    public final int port;
    public final boolean useXForwardedFor;
    public final boolean serveStaticResources;
    public final boolean debugRequests;
    public final Duration shutdownDelay;
    public final Duration shutdownTimeout;

    @JsonCreator
    ServerConfiguration(final Optional<String> host,
                        final int port,
                        @JsonProperty("use_x_forwarded_for") final boolean useXForwardedFor,
                        final boolean serveStaticResources,
                        final boolean debugRequests,
                        final Duration shutdownDelay,
                        final Duration shutdownTimeout) {
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.useXForwardedFor = useXForwardedFor;
        this.serveStaticResources = serveStaticResources;
        this.debugRequests = debugRequests;
        this.shutdownDelay = shutdownDelay;
        this.shutdownTimeout = shutdownTimeout;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("host", host)
                .add("port", port)
                .add("useXForwardedFor", useXForwardedFor)
                .add("serverStaticResources", serveStaticResources)
                .add("debugRequests", debugRequests)
                .add("shutdownDelay", shutdownDelay)
                .add("shutdownTimeout", shutdownTimeout)
                .toString();
    }
}
