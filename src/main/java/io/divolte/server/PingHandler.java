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

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.charset.StandardCharsets;

import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

/**
 * Event handler for ping requests.
 *
 * Ping requests are immediately and always responded to with a "pong" text response.
 */
@ParametersAreNonnullByDefault
final class PingHandler implements HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private volatile boolean shutdown;

    public void shutdown() {
        this.shutdown = true;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) {
        logger.debug("Health check from {}", exchange.getSourceAddress().getHostString());
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        if (shutdown) {
            logger.debug("Health check indicating unavailable; shutdown has commenced.");
            // If we started a shutdown, we want the health check to return a 503 to
            // indicate that we are shutting down and won't be available soon.
            exchange.setStatusCode(HTTP_UNAVAILABLE)
                    .getResponseSender().send("No p*ng for you, shutting down", StandardCharsets.UTF_8);
        } else {
            exchange.getResponseSender().send("pong", StandardCharsets.UTF_8);
        }
    }
}
