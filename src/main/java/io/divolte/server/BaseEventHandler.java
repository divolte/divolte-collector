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

import io.divolte.server.CookieValues.CookieValue;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;
import io.undertow.util.ETag;
import io.undertow.util.ETagUtils;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.io.Resources;

@ParametersAreNonnullByDefault
public abstract class BaseEventHandler implements HttpHandler {
    private static Logger logger = LoggerFactory.getLogger(BaseEventHandler.class);

    public static final AttachmentKey<CookieValue> PARTY_COOKIE_KEY = AttachmentKey.create(CookieValue.class);
    public static final AttachmentKey<CookieValue> SESSION_COOKIE_KEY = AttachmentKey.create(CookieValue.class);
    public static final AttachmentKey<String> PAGE_VIEW_ID_KEY = AttachmentKey.create(String.class);
    public static final AttachmentKey<String> EVENT_ID_KEY = AttachmentKey.create(String.class);
    public static final AttachmentKey<Long> REQUEST_START_TIME_KEY = AttachmentKey.create(Long.class);
    public static final AttachmentKey<Long> COOKIE_UTC_OFFSET_KEY = AttachmentKey.create(Long.class);
    public static final AttachmentKey<Boolean> FIRST_IN_SESSION_KEY = AttachmentKey.create(Boolean.class);

    public static final AttachmentKey<Optional<String>> LOCATION_KEY = AttachmentKey.create(Optional.class);
    public static final AttachmentKey<Optional<String>> REFERER_KEY = AttachmentKey.create(Optional.class);
    public static final AttachmentKey<Optional<String>> EVENT_TYPE_KEY = AttachmentKey.create(Optional.class);
    public static final AttachmentKey<Optional<Integer>> VIEWPORT_PIXEL_WIDTH_KEY = AttachmentKey.create(Optional.class);
    public static final AttachmentKey<Optional<Integer>> VIEWPORT_PIXEL_HEIGHT_KEY = AttachmentKey.create(Optional.class);
    public static final AttachmentKey<Optional<Integer>> SCREEN_PIXEL_WIDTH_KEY = AttachmentKey.create(Optional.class);
    public static final AttachmentKey<Optional<Integer>> SCREEN_PIXEL_HEIGHT_KEY = AttachmentKey.create(Optional.class);
    public static final AttachmentKey<Optional<Integer>> DEVICE_PIXEL_RATIO_KEY = AttachmentKey.create(Optional.class);
    public static final AttachmentKey<Function<String,Optional<String>>> EVENT_PARAM_PRODUCER_KEY = AttachmentKey.create(Function.class);

    private final static ETag SENTINEL_ETAG = new ETag(false, "6b3edc43-20ec-4078-bc47-e965dd76b88a");
    private final static String SENTINEL_ETAG_VALUE = SENTINEL_ETAG.toString();

    private final ByteBuffer transparentImage;
    protected final IncomingRequestProcessingPool processingPool;

    public BaseEventHandler(final IncomingRequestProcessingPool processingPool) {
        this.processingPool = Objects.requireNonNull(processingPool);

        try {
            this.transparentImage = ByteBuffer.wrap(
                Resources.toByteArray(Resources.getResource("transparent1x1.gif"))
            ).asReadOnlyBuffer();
        } catch (final IOException e) {
            // Should throw something more specific than this.
            throw new RuntimeException("Could not load transparent image resource.", e);
        }
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) {
        /*
         * The source address can be fetched on-demand from the peer connection, which may
         * no longer be available after the response has been sent. So we materialize it here
         * to ensure it's available further down the chain.
         */
        final InetSocketAddress sourceAddress = exchange.getSourceAddress();
        exchange.setSourceAddress(sourceAddress);

        /*
         * Set up the headers that we always send as a response, irrespective of what type it
         * will be. Note that the client is responsible for ensuring that ensures that each request
         * is unique.
         * The cache-related headers are intended to prevent spurious reloads for an event.
         * (Being a GET request, agents are free to re-issue the request at will. We don't want this.)
         * As a last resort, we try to detect duplicates via the ETag header.
         */
        exchange.getResponseHeaders()
                .put(Headers.CONTENT_TYPE, "image/gif")
                .put(Headers.ETAG, SENTINEL_ETAG_VALUE)
                .put(Headers.CACHE_CONTROL, "private, no-cache, proxy-revalidate")
                .put(Headers.PRAGMA, "no-cache")
                .put(Headers.EXPIRES, "Fri, 14 Apr 1995 11:30:00 GMT");

        // If an ETag is present, this is a duplicate event.
        if (ETagUtils.handleIfNoneMatch(exchange, SENTINEL_ETAG, true)) {
            /*
             * Subclasses are responsible to logging events.
             * We just ensure the pixel is always returned, no matter what.
             */
            try {
                logEvent(exchange);
            } finally {
                exchange.setResponseCode(StatusCodes.OK);
                exchange.getResponseSender().send(transparentImage.slice());
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Ignoring duplicate event from {}: {}", sourceAddress, getFullUrl(exchange));
            }
            exchange.setResponseCode(StatusCodes.NOT_MODIFIED);
            exchange.endExchange();
        }
    }

    private static String getFullUrl(HttpServerExchange exchange) {
        final String queryString = exchange.getQueryString();
        final String requestUrl = exchange.getRequestURL();
        return Strings.isNullOrEmpty(queryString)
                ? requestUrl
                : requestUrl + '?' + queryString;
    }

    /**
     * Log this event.
     *
     * The subclass is responsible for extracting all information from the request and
     * handing it off. The client is still waiting at this point; the subclass should hand
     * further processing of as expediently as possible. When it returns (or throws an
     * exception) the pixel response will be sent. (The subclass must never complete the
     * request.)
     * @param exchange the HTTP exchange from which event data can be extracted.
     */
    protected abstract void logEvent(final HttpServerExchange exchange);

    protected static class IncompleteRequestException extends Exception {
        private static final long serialVersionUID = 1L;
    }
}
