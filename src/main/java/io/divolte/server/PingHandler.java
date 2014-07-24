package io.divolte.server;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

class PingHandler implements HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public static final PingHandler HANDLER = new PingHandler();

    private PingHandler() {
        // Prevent external instantiation.
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        logger.debug("Ping received from {}", exchange.getDestinationAddress().getHostString());
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        exchange.getResponseSender().send("pong", StandardCharsets.UTF_8);
    }
}
