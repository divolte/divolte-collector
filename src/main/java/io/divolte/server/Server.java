package io.divolte.server;

import io.undertow.Undertow;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class Server implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final Undertow undertow;

    public Server(final String host) {
        final PathHandler handler = new PathHandler();
        handler.addExactPath("/ping", (exchange) -> {
            logger.debug("PING!");
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
            exchange.getResponseSender().send("pong", StandardCharsets.UTF_8);
        });

        undertow = Undertow.builder()
                           .addHttpListener(1234, host)
                           .setHandler(handler)
                           .build();
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("Stopping server.");
                    undertow.stop();
                }
        ));
        logger.info("Starting server.");
        undertow.start();
    }

    public static void main(final String[] args) {
        new Server("localhost").run();
    }
}
