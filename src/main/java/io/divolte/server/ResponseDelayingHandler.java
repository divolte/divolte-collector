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

package io.divolte.server;

import io.undertow.server.ConduitWrapper;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.ConduitFactory;
import org.xnio.conduits.StreamSinkConduit;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Handler that delays responses.
 */
@ParametersAreNonnullByDefault
public class ResponseDelayingHandler implements HttpHandler {

    private final HttpHandler next;
    private final long delayMilliseconds;

    private final ConduitWrapper<StreamSinkConduit> WRAPPER = new ConduitWrapper<StreamSinkConduit>() {
        @Override
        public StreamSinkConduit wrap(final ConduitFactory<StreamSinkConduit> factory, final HttpServerExchange exchange) {
            return new DelayingStreamSinkConduit(factory.create(), delayMilliseconds, TimeUnit.MILLISECONDS);
        }
    };

    public ResponseDelayingHandler(final HttpHandler next, final long delay, final TimeUnit timeUnit) {
        this.next = Objects.requireNonNull(next);
        this.delayMilliseconds = timeUnit.toMillis(delay);
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        exchange.addResponseWrapper(WRAPPER);
        next.handleRequest(exchange);
    }
}
