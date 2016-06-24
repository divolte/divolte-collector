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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Strings;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Resources;

import io.divolte.server.DivolteEvent.BrowserEventData;
import io.divolte.server.mincode.MincodeFactory;
import io.divolte.server.processing.Item;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.ETag;
import io.undertow.util.ETagUtils;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import static io.divolte.server.HttpSource.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;

@ParametersAreNonnullByDefault
public final class ClientSideCookieEventHandler implements HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ClientSideCookieEventHandler.class);

    private final static ETag SENTINEL_ETAG = new ETag(false, "6b3edc43-20ec-4078-bc47-e965dd76b88a");
    private final static String SENTINEL_ETAG_VALUE = SENTINEL_ETAG.toString();

    private final ByteBuffer transparentImage;
    private final IncomingRequestProcessingPool processingPool;
    private final int sourceIndex;

    private static final String TRUE_STRING = "t";

    private static final String PARTY_ID_QUERY_PARAM = "p";
    private static final String NEW_PARTY_ID_QUERY_PARAM = "n";
    private static final String SESSION_ID_QUERY_PARAM = "s";
    private static final String FIRST_IN_SESSION_QUERY_PARAM = "f";
    private static final String EVENT_ID_QUERY_PARAM = "e";
    private static final String CLIENT_TIMESTAMP_QUERY_PARAM = "c"; // chronos
    private static final String CHECKSUM_QUERY_PARAM = "x";
    private static final String PAGE_VIEW_ID_QUERY_PARAM = "v";
    private static final String EVENT_TYPE_QUERY_PARAM = "t";
    private static final String EVENT_PARAMETERS_QUERY_PARAM = "u";
    private static final String LOCATION_QUERY_PARAM = "l";
    private static final String REFERER_QUERY_PARAM = "r";
    private static final String VIEWPORT_PIXEL_WIDTH_QUERY_PARAM = "w";
    private static final String VIEWPORT_PIXEL_HEIGHT_QUERY_PARAM = "h";
    private static final String SCREEN_PIXEL_WIDTH_QUERY_PARAM = "i";
    private static final String SCREEN_PIXEL_HEIGHT_QUERY_PARAM = "j";
    private static final String DEVICE_PIXEL_RATIO_QUERY_PARAM = "k";

    private static final ObjectReader EVENT_PARAMETERS_READER = new ObjectMapper(new MincodeFactory()).reader();

    public ClientSideCookieEventHandler(final IncomingRequestProcessingPool processingPool, final int sourceIndex) {
        this.sourceIndex = sourceIndex;
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
        final InetSocketAddress sourceAddress = captureAndPersistSourceAddress(exchange);

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
            // Default status code what we want: 200 OK.
            // Sending the response before logging the event!
            exchange.getResponseSender().send(transparentImage.slice());

            try {
                logEvent(exchange);
            } catch (final IncompleteRequestException ire) {
                // improper request, could be anything
                logger.warn("Improper request received from {}.", Optional.ofNullable(exchange.getSourceAddress()).map(InetSocketAddress::getHostString).orElse("<UNKNOWN HOST>"));
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Ignoring duplicate event from {}: {}", sourceAddress, getFullUrl(exchange));
            }
            exchange.setStatusCode(StatusCodes.NOT_MODIFIED);
            exchange.endExchange();
        }
    }

    private static String getFullUrl(final HttpServerExchange exchange) {
        final String queryString = exchange.getQueryString();
        final String requestUrl = exchange.getRequestURL();
        return Strings.isNullOrEmpty(queryString)
                ? requestUrl
                : requestUrl + '?' + queryString;
    }

    private void logEvent(final HttpServerExchange exchange) throws IncompleteRequestException {
        final DivolteIdentifier partyId = queryParamFromExchange(exchange, PARTY_ID_QUERY_PARAM).flatMap(DivolteIdentifier::tryParse).orElseThrow(IncompleteRequestException::new);
        final UndertowEvent event = new BrowserUndertowEvent(Instant.now(), exchange, partyId);
        processingPool.enqueue(Item.of(sourceIndex, partyId.value, event));
    }

    private static final class BrowserUndertowEvent extends UndertowEvent {
        private BrowserUndertowEvent(final Instant requestTime, final HttpServerExchange exchange, final DivolteIdentifier partyId) {
            super(requestTime, exchange, partyId);
        }

        @Override
        public DivolteEvent parseRequest() throws IncompleteRequestException {
            final boolean corrupt = !isRequestChecksumCorrect(exchange);
            final DivolteIdentifier partyId = queryParamFromExchange(exchange, PARTY_ID_QUERY_PARAM).flatMap(DivolteIdentifier::tryParse).orElseThrow(IncompleteRequestException::new);
            final DivolteIdentifier sessionId = queryParamFromExchange(exchange, SESSION_ID_QUERY_PARAM).flatMap(DivolteIdentifier::tryParse).orElseThrow(IncompleteRequestException::new);
            final String pageViewId = queryParamFromExchange(exchange, PAGE_VIEW_ID_QUERY_PARAM).orElseThrow(IncompleteRequestException::new);
            final String eventId = queryParamFromExchange(exchange, EVENT_ID_QUERY_PARAM).orElseThrow(IncompleteRequestException::new);
            final boolean isNewPartyId = queryParamFromExchange(exchange, NEW_PARTY_ID_QUERY_PARAM).map(TRUE_STRING::equals).orElseThrow(IncompleteRequestException::new);
            final boolean isFirstInSession = queryParamFromExchange(exchange, FIRST_IN_SESSION_QUERY_PARAM).map(TRUE_STRING::equals).orElseThrow(IncompleteRequestException::new);
            final Instant clientTimeStamp = Instant.ofEpochMilli(queryParamFromExchange(exchange, CLIENT_TIMESTAMP_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Long).orElseThrow(IncompleteRequestException::new));

            final DivolteEvent event = DivolteEvent.createBrowserEvent(exchange, corrupt, partyId, sessionId, eventId,
                                                                       requestTime, clientTimeStamp,
                                                                       isNewPartyId, isFirstInSession,
                                                                       queryParamFromExchange(exchange, EVENT_TYPE_QUERY_PARAM),
                                                                       eventParameterSupplier(exchange),
                                                                       browserEventData(exchange, pageViewId));
            return event;
        }
    }

    private static Supplier<Optional<JsonNode>> eventParameterSupplier(final HttpServerExchange exchange) {
        return () -> queryParamFromExchange(exchange, EVENT_PARAMETERS_QUERY_PARAM)
                .map(encodedParameters -> {
                    try {
                        return EVENT_PARAMETERS_READER.readTree(encodedParameters);
                    } catch (final IOException e) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Could not parse custom event parameters: " + encodedParameters, e);
                        }
                        return null;
                    }
                });
    }

    private static BrowserEventData browserEventData(final HttpServerExchange exchange, final String pageViewId) {
        return new DivolteEvent.BrowserEventData(
                pageViewId,
                queryParamFromExchange(exchange, LOCATION_QUERY_PARAM),
                queryParamFromExchange(exchange, REFERER_QUERY_PARAM),
                queryParamFromExchange(exchange, VIEWPORT_PIXEL_WIDTH_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Int),
                queryParamFromExchange(exchange, VIEWPORT_PIXEL_HEIGHT_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Int),
                queryParamFromExchange(exchange, SCREEN_PIXEL_WIDTH_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Int),
                queryParamFromExchange(exchange, SCREEN_PIXEL_HEIGHT_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Int),
                queryParamFromExchange(exchange, DEVICE_PIXEL_RATIO_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Int));
    }

    private static final HashFunction CHECKSUM_HASH = Hashing.murmur3_32();

    private static boolean isRequestChecksumCorrect(final HttpServerExchange exchange) {
        // This is not intended to be robust against intentional tampering; it is intended to guard
        // against proxies and the like that may have truncated the request.

        return queryParamFromExchange(exchange, CHECKSUM_QUERY_PARAM)
                .map(ClientSideCookieEventHandler::tryParseBase36Long)
                .map((expectedChecksum) -> {
                    /*
                     * We could optimize this by calculating the checksum directly, instead of building up
                     * the intermediate string representation. For now the debug value of the string exceeds
                     * the benefits of going slightly faster.
                     */
                    final String canonicalRequestString = buildNormalizedChecksumString(exchange.getQueryParameters());
                    final int requestChecksum =
                            CHECKSUM_HASH.hashString(canonicalRequestString, StandardCharsets.UTF_8).asInt();
                    final boolean isRequestChecksumCorrect = expectedChecksum == requestChecksum;
                    if (!isRequestChecksumCorrect && logger.isDebugEnabled()) {
                        logger.debug("Checksum mismatch detected; expected {} but was {} for request string: {}",
                                Long.toString(expectedChecksum, 36),
                                Integer.toString(requestChecksum, 36),
                                canonicalRequestString);
                    }
                    return isRequestChecksumCorrect;
                })
                .orElse(false);
    }

    private static String buildNormalizedChecksumString(final Map<String,Deque<String>> queryParameters) {
        return buildNormalizedChecksumString(queryParameters instanceof SortedMap
                ? (SortedMap)queryParameters
                : new TreeMap<>(queryParameters));
    }

    private static String buildNormalizedChecksumString(final SortedMap<String,Deque<String>> queryParameters) {
        /*
         * Build up a canonical representation of the query parameters. The canonical order is:
         *  1) Sort the query parameters by key, preserving multiple values (and their order).
         *  2) The magic parameter containing the checksum is discarded.
         *  3) Build up a string. For each parameter:
         *     a) Append the parameter name, followed by a '='.
         *     b) Append each value of the parameter, followed by a ','.
         *     c) Append a ';'.
         *  This is designed to be unambiguous in the face of many edge cases.
         */
        final StringBuilder builder = new StringBuilder();
        queryParameters.forEach((name, values) -> {
            if (!CHECKSUM_QUERY_PARAM.equals(name)) {
                builder.append(name).append('=');
                values.forEach((value) -> builder.append(value).append(','));
                builder.append(';');
            }
        });
        return builder.toString();
    }

    @Nullable
    static Long tryParseBase36Long(final String input) {
        try {
            return Long.parseLong(input, 36);
        } catch(final NumberFormatException nfe) {
            return null;
        }
    }

    @Nullable
    private static Integer tryParseBase36Int(final String input) {
        try {
            return Integer.valueOf(input, 36);
        } catch (final NumberFormatException ignored) {
            // We expect parsing to fail; signal via null.
            return null;
        }
    }
}
