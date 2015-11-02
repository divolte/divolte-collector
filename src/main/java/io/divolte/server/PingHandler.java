/*
 * Copyright 2014 GoDataDriven B.V.
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
import io.undertow.util.Headers;

import java.nio.charset.StandardCharsets;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event handler for ping requests.
 *
 * Ping requests are immediately and always responded to with a "pong" text response.
 */
@ParametersAreNonnullByDefault
final class PingHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private PingHandler() {
        // Prevent external instantiation.
    }

    public static void handlePingRequest(final HttpServerExchange exchange) {
        logger.debug("Ping received from {}", exchange.getSourceAddress().getHostString());
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        exchange.getResponseSender().send("pong", StandardCharsets.UTF_8);
    }
}
