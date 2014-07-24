package io.divolte.server;

import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import com.typesafe.config.Config;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.Cookie;
import io.undertow.server.handlers.CookieImpl;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import io.undertow.util.StatusCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

final class DivolteEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final String partyCookieName;
    private final Duration partyTimeout;
    private final String sessionCookieName;
    private final Duration sessionTimeout;

    private final ByteBuffer transparentImage;

    public DivolteEventHandler(final String partyCookieName,
                               final Duration partyTimeout,
                               final String sessionCookieName,
                               final Duration sessionTimeout) {
        this.partyCookieName = Preconditions.checkNotNull(partyCookieName);
        this.partyTimeout = Preconditions.checkNotNull(partyTimeout);
        this.sessionCookieName = Preconditions.checkNotNull(sessionCookieName);
        this.sessionTimeout = Preconditions.checkNotNull(sessionTimeout);
        try {
            this.transparentImage = ByteBuffer.wrap(
                Resources.toByteArray(Resources.getResource("transparent1x1.png"))
            );
        } catch (final IOException e) {
            // Should throw something more specific than this.
            throw new RuntimeException("Could not load transparent image resource.", e);
        }
    }

    public DivolteEventHandler(final Config config) {
        this(config.getString("divolte.tracking.party_cookie"),
             Duration.ofSeconds(config.getDuration("divolte.tracking.party_timeout", TimeUnit.SECONDS)),
             config.getString("divolte.tracking.session_cookie"),
             Duration.ofSeconds(config.getDuration("divolte.tracking.session_timeout", TimeUnit.SECONDS)));
    }

    public void handleEventRequest(final HttpServerExchange exchange) throws Exception {
        // We only accept GET requests.
        /*
         * Our strategy is:
         * 1) Set up the cookies.
         * 2) Acknowledge the response.
         * 3) Pass into our queuing system for further handling.
         */
        if (exchange.getRequestMethod().equals(Methods.GET)) {
            final String partyId = getTrackingIdentifier(exchange, partyCookieName, partyTimeout);
            final String sessionId = getTrackingIdentifier(exchange, sessionCookieName, sessionTimeout);

            exchange.setResponseCode(StatusCodes.ACCEPTED);
            serveImage(exchange);

            logger.info("Event received: {}/{}", partyId, sessionId);
        } else {
            methodNotAllowed(exchange);
        }
    }

    private void methodNotAllowed(final HttpServerExchange exchange) {
        final HeaderMap responseHeaders = exchange.getResponseHeaders();
        responseHeaders.put(Headers.ALLOW, Methods.GET_STRING);
        responseHeaders.put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        exchange.setResponseCode(StatusCodes.METHOD_NOT_ALLOWED);
        exchange.getResponseSender()
                .send("HTTP method " + exchange.getRequestMethod() + " not allowed.", StandardCharsets.UTF_8);
    }

    private void serveImage(final HttpServerExchange exchange) {
        final HeaderMap responseHeaders = exchange.getResponseHeaders();
        responseHeaders.put(Headers.CONTENT_TYPE, "image/png");
        responseHeaders.put(Headers.CACHE_CONTROL, "no-cache, no-store, must-revalidate");
        responseHeaders.put(Headers.PRAGMA, "no-cache");
        responseHeaders.put(Headers.EXPIRES, 0);
        exchange.getResponseSender().send(transparentImage);
    }

    private static String getTrackingIdentifier(final HttpServerExchange exchange,
                                                final String cookieName,
                                                final Duration timeout) {
        final Cookie trackingCookie = exchange.getRequestCookies().computeIfAbsent(cookieName, (name) ->
                new CookieImpl(name, UUID.randomUUID().toString())
        );
        trackingCookie.setVersion(1);
        final long maxAge = timeout.getSeconds();
        if (maxAge <= Integer.MAX_VALUE) {
            trackingCookie.setMaxAge((int) maxAge);
        }
        trackingCookie.setExpires(new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(maxAge)));
        exchange.setResponseCookie(trackingCookie);
        return trackingCookie.getValue();
    }
}
