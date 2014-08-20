package io.divolte.server;

import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.Cookie;
import io.undertow.server.handlers.CookieImpl;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import io.undertow.util.StatusCodes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.typesafe.config.Config;

/**
 * Event handler for Divolte signalling events.
 *
 * This handler deals with requests aimed at our signalling endpoint. The signalling
 * endpoint responds to GET requests with a small transparent 1x1 image, allowing it
 * to be invoked using image requests.
 *
 * Handling consists of:
 * <ul>
 *   <li>Ensures the tracking cookies are set. We have a long-lived 'party' cookie,
 *     that tracks a client across sessions, and a short-lived 'session' cookie that
 *     tracks a client for the duration of a single session.</li>
 *   <li>Issues a page-view ID if one was not supplied with the request.</li>
 *   <li>Responds (immediately) to the request with an small transparent 1x1 image.
 *     Headers are set to try to ensure that the request <em>cannot</em> be cached.
 *   </li>
 *   <li>Hands off the request (via the processing pool) for further processing.</li>
 * </ul>
 */
@ParametersAreNonnullByDefault
final class DivolteEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final String partyCookieName;
    private final Duration partyTimeout;
    private final String sessionCookieName;
    private final Duration sessionTimeout;
    private final String pageViewCookieName;

    private final ByteBuffer transparentImage;

    private final IncomingRequestProcessingPool processingPool;


    public DivolteEventHandler(final String partyCookieName,
                               final Duration partyTimeout,
                               final String sessionCookieName,
                               final Duration sessionTimeout,
                               final String pageViewCookieName,
                               final IncomingRequestProcessingPool processingPool) {
        this.partyCookieName =    Objects.requireNonNull(partyCookieName);
        this.partyTimeout =       Objects.requireNonNull(partyTimeout);
        this.sessionCookieName =  Objects.requireNonNull(sessionCookieName);
        this.sessionTimeout =     Objects.requireNonNull(sessionTimeout);
        this.pageViewCookieName = Objects.requireNonNull(pageViewCookieName);
        this.processingPool =     Objects.requireNonNull(processingPool);
        try {
            this.transparentImage = ByteBuffer.wrap(
                Resources.toByteArray(Resources.getResource("transparent1x1.gif"))
            ).asReadOnlyBuffer();
        } catch (final IOException e) {
            // Should throw something more specific than this.
            throw new RuntimeException("Could not load transparent image resource.", e);
        }
    }

    public DivolteEventHandler(final Config config) {
        this(config.getString("divolte.tracking.party_cookie"),
             Duration.ofSeconds(config.getDuration("divolte.tracking.party_timeout", TimeUnit.SECONDS)),
             config.getString("divolte.tracking.session_cookie"),
             Duration.ofSeconds(config.getDuration("divolte.tracking.session_timeout", TimeUnit.SECONDS)),
             config.getString("divolte.tracking.page_view_cookie"),
             new IncomingRequestProcessingPool(config));
    }

    public void handleEventRequest(final HttpServerExchange exchange) throws Exception {
        /*
         * Our strategy is:
         * 1) Set up the cookies.
         * 2) Materialize the source address, so we can safely get it later.
         * 3) Acknowledge the response.
         * 4) Pass into our queuing system for further handling.
         */
        // We only accept GET requests.
        if (exchange.getRequestMethod().equals(Methods.GET)) {
            // 1
            final String partyId = getTrackingIdentifier(exchange, partyCookieName, partyTimeout);
            final String sessionId = getTrackingIdentifier(exchange, sessionCookieName, sessionTimeout);
            final String pageViewId = setAndReturnPageViewCookie(exchange, pageViewCookieName);

            // 2
            /*
             * The source address can be fetched on-demand from the peer connection, which may
             * no longer be available after the response has been sent. So we materialize it here
             * to ensure it's available further down the chain.
             */
            exchange.setSourceAddress(exchange.getSourceAddress());

            // 3
            exchange.setResponseCode(StatusCodes.ACCEPTED);
            serveImage(exchange);

            // 4
            logger.debug("Enqueuing event: {}/{}/{}", partyId, sessionId, pageViewId);
            processingPool.enqueueIncomingExchangeForProcessing(partyId, exchange);
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
        responseHeaders.put(Headers.CONTENT_TYPE, "image/gif");
        responseHeaders.put(Headers.CACHE_CONTROL, "no-cache, no-store, must-revalidate");
        responseHeaders.put(Headers.PRAGMA, "no-cache");
        responseHeaders.put(Headers.EXPIRES, 0);
        exchange.getResponseSender().send(transparentImage.slice());
    }

    private static String getTrackingIdentifier(final HttpServerExchange exchange,
                                                final String cookieName,
                                                final Duration timeout) {
        Cookie trackingCookie = exchange.getRequestCookies().get(cookieName);
        if (null == trackingCookie) {
            final String cookieValue = UUID.randomUUID().toString();
            logger.debug("New {} cookie generated: {}", cookieName, cookieValue);
            trackingCookie = new CookieImpl(cookieName, cookieValue);
        }
        trackingCookie.setVersion(1);
        trackingCookie.setHttpOnly(true);
        final long maxAge = timeout.getSeconds();
        // Some clients (e.g. netty) choke if max-age is large than an Integer can represent.
        if (maxAge <= Integer.MAX_VALUE) {
            trackingCookie.setMaxAge((int) maxAge);
        }
        trackingCookie.setExpires(new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(maxAge)));
        exchange.setResponseCookie(trackingCookie);
        return trackingCookie.getValue();
    }

    private static String setAndReturnPageViewCookie(final HttpServerExchange exchange, final String cookieName) {
        final String cookieValue = Optional.ofNullable(exchange.getQueryParameters().get("p"))
                .map(Deque::getFirst)
                .orElseGet(() -> UUID.randomUUID().toString());

        final CookieImpl pageViewCookie = new CookieImpl(cookieName, cookieValue);
        pageViewCookie.setVersion(1);
        pageViewCookie.setHttpOnly(false);
        exchange.setResponseCookie(pageViewCookie);

        return cookieValue;
    }
}
