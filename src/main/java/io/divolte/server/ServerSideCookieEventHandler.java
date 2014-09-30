package io.divolte.server;

import io.divolte.server.CookieValues.CookieValue;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.Cookie;
import io.undertow.server.handlers.CookieImpl;
import io.undertow.util.AttachmentKey;

import java.time.Duration;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

/**
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
final class ServerSideCookieEventHandler extends BaseEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final String partyCookieName;
    private final Duration partyTimeout;
    private final String sessionCookieName;
    private final Duration sessionTimeout;
    private final OptionalConfig<String> cookieDomain;


    public ServerSideCookieEventHandler(final String partyCookieName,
                               final Duration partyTimeout,
                               final String sessionCookieName,
                               final Duration sessionTimeout,
                               final OptionalConfig<String> cookieDomain,
                               final IncomingRequestProcessingPool processingPool) {
        super(processingPool);
        this.partyCookieName =    Objects.requireNonNull(partyCookieName);
        this.partyTimeout =       Objects.requireNonNull(partyTimeout);
        this.sessionCookieName =  Objects.requireNonNull(sessionCookieName);
        this.sessionTimeout =     Objects.requireNonNull(sessionTimeout);
        this.cookieDomain = cookieDomain;
    }

    public ServerSideCookieEventHandler(final Config config, final IncomingRequestProcessingPool pool) {
        this(config.getString("divolte.tracking.party_cookie"),
             Duration.ofSeconds(config.getDuration("divolte.tracking.party_timeout", TimeUnit.SECONDS)),
             config.getString("divolte.tracking.session_cookie"),
             Duration.ofSeconds(config.getDuration("divolte.tracking.session_timeout", TimeUnit.SECONDS)),
             OptionalConfig.of(config::getString, "divolte.tracking.cookie_domain"),
             pool);
    }

    protected void doHandleEventRequest(final HttpServerExchange exchange) throws Exception {
        /*
         * Our strategy is:
         * 1) Set up the cookies.
         * 2) Acknowledge the response.
         * 3) Pass into our queuing system for further handling.
         */

        // 1
        final long requestTime = System.currentTimeMillis();
        exchange.putAttachment(REQUEST_START_TIME_KEY, requestTime);
        // Server side generated cookies are UTC, so offset = 0
        exchange.putAttachment(COOKIE_UTC_OFFSET_KEY, 0L);

        final CookieValue partyId = prepareTrackingIdentifierAndReturnCookieValue(exchange, partyCookieName, PARTY_COOKIE_KEY, partyTimeout, requestTime);
        final CookieValue sessionId = prepareTrackingIdentifierAndReturnCookieValue(exchange, sessionCookieName, SESSION_COOKIE_KEY, sessionTimeout, requestTime);

        final String pageViewId = queryParamFromExchange(exchange, PAGE_VIEW_ID_QUERY_PARAM).orElseGet(() -> CookieValues.generate(requestTime).value);
        exchange.putAttachment(PAGE_VIEW_ID_KEY, pageViewId);
        exchange.putAttachment(EVENT_ID_KEY, pageViewId);

        // required for the RecordMapper; the logic to determine whether a event is first in session
        // differs between the server side and client side cookie endpoint
        exchange.putAttachment(FIRST_IN_SESSION_KEY, !exchange.getRequestCookies().containsKey(sessionCookieName));

        // 2
        serveImage(exchange);

        // 3
        logger.debug("Enqueuing event (server generated cookies): {}/{}/{}", partyId, sessionId, pageViewId);
        processingPool.enqueueIncomingExchangeForProcessing(partyId, exchange);
    }

    private CookieValue prepareTrackingIdentifierAndReturnCookieValue(final HttpServerExchange exchange,
                                                final String cookieName,
                                                final AttachmentKey<CookieValue> key,
                                                final Duration timeout,
                                                final long currentTime) {

        final Cookie existingCookie = exchange.getRequestCookies().get(cookieName);

        final CookieValue cookieValue = Optional.ofNullable(existingCookie)
        .map(Cookie::getValue)
        .flatMap(CookieValues::tryParse)
        .orElseGet(() -> CookieValues.generate(currentTime));

        final Cookie newCookie = new CookieImpl(cookieName, cookieValue.value);
        cookieDomain.ifPresent(newCookie::setDomain);

        final long maxAge = timeout.getSeconds();
        // Some clients (e.g. netty) choke if max-age is large than an Integer can represent.
        if (maxAge <= Integer.MAX_VALUE) {
            newCookie.setMaxAge((int) maxAge);
        }

        newCookie
        .setVersion(1)
        .setHttpOnly(true)
        .setExpires(new Date(currentTime + TimeUnit.SECONDS.toMillis(maxAge)));

        exchange
        .setResponseCookie(newCookie)
        .putAttachment(key, cookieValue);

        return cookieValue;
    }
}
