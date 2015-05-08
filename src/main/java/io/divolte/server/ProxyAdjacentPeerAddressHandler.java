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

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyAdjacentPeerAddressHandler implements HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ProxyAdjacentPeerAddressHandler.class);

    private final HttpHandler next;

    public ProxyAdjacentPeerAddressHandler(HttpHandler next) {
        this.next = next;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        Optional
        .ofNullable(exchange.getRequestHeaders().getLast(Headers.X_FORWARDED_FOR))
        .ifPresent((forwardedFor) -> {
            int index = forwardedFor.lastIndexOf(',');
            final String value;
            if (index == -1) {
                value = forwardedFor;
            } else {
                value = forwardedFor.substring(index + 1, forwardedFor.length()).trim();
            }
            try {
                final InetAddress address = InetAddress.getByName(value);
                //we have no way of knowing the port
                exchange.setSourceAddress(new InetSocketAddress(address, 0));
            } catch (Exception e) {
                logger.warn("Received X-Forwarded-For header with unparseable IP address.");
            }
        });

        Optional.ofNullable(exchange.getRequestHeaders().getFirst(Headers.X_FORWARDED_PROTO)).ifPresent(exchange::setRequestScheme);

        next.handleRequest(exchange);
    }
}
