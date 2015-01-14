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

import static io.divolte.server.IncomingRequestProcessor.EVENT_DATA_KEY;
import static io.divolte.server.QueryParameterNames.PAGE_VIEW_ID_QUERY_PARAM;

import io.divolte.server.CookieValues.CookieValue;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.Cookie;
import io.undertow.server.handlers.CookieImpl;

import java.time.Duration;
import java.util.*;
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

    protected void logEvent(final HttpServerExchange exchange) {
        /*
         * Our strategy is:
         * 1) Set up the cookies.
         * 2) Pass into our queuing system for further handling.
         *
         * The caller will then complete the HTTP response.
         */

        // 1
        final long requestTime = System.currentTimeMillis();
        final CookieValue partyId = prepareTrackingIdentifierAndReturnCookieValue(exchange, partyCookieName, partyTimeout, requestTime);
        final CookieValue sessionId = prepareTrackingIdentifierAndReturnCookieValue(exchange, sessionCookieName, sessionTimeout, requestTime);
        final String pageViewId = queryParamFromExchange(exchange, PAGE_VIEW_ID_QUERY_PARAM).orElseGet(() -> CookieValues.generate(requestTime).value);

        // required for the RecordMapper; the logic to determine whether a event is first in session
        // differs between the server side and client side cookie endpoint
        final Map<String, Cookie> requestCookies = exchange.getRequestCookies();
        final boolean firstInSession = !requestCookies.containsKey(sessionCookieName);
        final boolean newPartyId = !requestCookies.containsKey(partyCookieName);

        // Server side generated cookies are UTC, so offset = 0
        final EventData eventData = new EventData(false, partyId, sessionId, pageViewId, pageViewId,
                                                  requestTime, 0L, newPartyId, firstInSession, exchange);
        exchange.putAttachment(EVENT_DATA_KEY, eventData);

        // 2
        logger.debug("Enqueuing event (server generated cookies): {}/{}/{}", partyId, sessionId, pageViewId);
        processingPool.enqueueIncomingExchangeForProcessing(partyId, exchange);
    }

    private CookieValue prepareTrackingIdentifierAndReturnCookieValue(final HttpServerExchange exchange,
                                                                      final String cookieName,
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

        exchange.setResponseCookie(newCookie);

        return cookieValue;
    }
}
