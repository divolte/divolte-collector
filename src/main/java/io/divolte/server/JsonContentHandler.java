/*
 * Copyright 2016 GoDataDriven B.V.
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
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;

@ParametersAreNonnullByDefault
public class JsonContentHandler implements HttpHandler {

    private final HttpHandler next;

    public JsonContentHandler(final HttpHandler next) {
        this.next = Objects.requireNonNull(next);
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        final HeaderValues contentType = exchange.getRequestHeaders().get(Headers.CONTENT_TYPE);
        if (null != contentType
                && contentType.size() == 1
                && contentType.getFirst().toLowerCase(Locale.ROOT).equals("application/json")) {
            next.handleRequest(exchange);
        } else {
            exchange.setStatusCode(StatusCodes.UNSUPPORTED_MEDIA_TYPE);
            exchange.getResponseHeaders()
                    .put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
            exchange.getResponseSender()
                    .send("Content type must be application/json.", StandardCharsets.UTF_8);
            exchange.endExchange();
        }
    }
}
