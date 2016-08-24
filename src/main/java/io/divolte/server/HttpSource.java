/*
 * Copyright 2015 GoDataDriven B.V.
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

package io.divolte.server;

import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.PathHandler;

import javax.annotation.ParametersAreNonnullByDefault;

import java.net.InetSocketAddress;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public abstract class HttpSource {
    protected final String sourceName;

    protected HttpSource(final String sourceName) {
        this.sourceName = Objects.requireNonNull(sourceName);
    }

    public abstract PathHandler attachToPathHandler(PathHandler pathHandler);

    protected static InetSocketAddress captureAndPersistSourceAddress(final HttpServerExchange exchange) {
        /*
         * The source address can be fetched on-demand from the peer connection, which may
         * no longer be available after the response has been sent. So we materialize it here
         * to ensure it's available further down the chain.
         */
        final InetSocketAddress sourceAddress = exchange.getSourceAddress();
        exchange.setSourceAddress(sourceAddress);
        return sourceAddress;
    }

    public static Optional<String> queryParamFromExchange(final HttpServerExchange exchange, final String param) {
        return Optional.ofNullable(exchange.getQueryParameters().get(param)).map(Deque::getFirst);
    }
}
