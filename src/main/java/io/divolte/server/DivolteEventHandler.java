package io.divolte.server;

import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.Cookie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

final class DivolteEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    // TODO: Make these configurable
    private static final String PARTY_COOKIE_NAME = "_dvp";
    private static final String SESSION_COOKIE_NAME = "_dvs";
    private static final Duration SESSION_TIMEOUT = Duration.ofMinutes(30);

    public void handleEventRequest(final HttpServerExchange exchange) throws Exception {
        /*
         * Our strategy is:
         * 1) Set up the cookies.
         * 2) Acknowledge the response.
         * 3) Pass into our queuing system for further handling.
         */

    }
}
