package io.divolte.server;

import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;

import java.nio.charset.StandardCharsets;

@ParametersAreNonnullByDefault
final class PingHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private PingHandler() {
        // Prevent external instantiation.
    }

    public static void handlePingRequest(final HttpServerExchange exchange) throws Exception {
        logger.debug("Ping received from {}", exchange.getDestinationAddress().getHostString());
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        exchange.getResponseSender().send("pong", StandardCharsets.UTF_8);
    }
}
