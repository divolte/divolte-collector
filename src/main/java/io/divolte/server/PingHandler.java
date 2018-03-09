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

import java.nio.charset.StandardCharsets;
import javax.annotation.ParametersAreNonnullByDefault;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public PingHandler() {
        // Prevent external instantiation.
        shutdown = false;
    }

    public void shutdown() {
        this.shutdown = true;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        logger.debug("Health check from {}", exchange.getSourceAddress().getHostString());
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        if(shutdown) {
            // If we started a shutdown, we want the health check to a 503 to let upstream
            // know that we are shutting down and it should be removed from the pool
            exchange.setStatusCode(HTTP_UNAVAILABLE);
            exchange.getResponseSender().send("No p*ng for you, shutting down", StandardCharsets.UTF_8);

            logger.info("Return 503 on health check to indicate shutdown");
        } else {
            exchange.getResponseSender().send("pong", StandardCharsets.UTF_8);
        }
    }
}
