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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.divolte.server.mincode.MincodeFactory;
import io.undertow.server.HttpServerExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.divolte.server.IncomingRequestProcessor.DIVOLTE_EVENT_KEY;

@ParametersAreNonnullByDefault
public final class ClientSideCookieEventHandler extends BaseEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(ClientSideCookieEventHandler.class);
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

    static final String EVENT_SOURCE_NAME = "browser";

    public ClientSideCookieEventHandler(final IncomingRequestProcessingPool pool) {
        super(pool);
    }

    @Override
    protected void logEvent(HttpServerExchange exchange) {
        try {
            handleRequestIfComplete(exchange);
        } catch (final IncompleteRequestException ire) {
            // improper request, could be anything
            logger.warn("Improper request received from {}.", Optional.ofNullable(exchange.getSourceAddress()).map(InetSocketAddress::getHostString).orElse("<UNKNOWN HOST>"));
        }
    }

    private void handleRequestIfComplete(final HttpServerExchange exchange) throws IncompleteRequestException {
        final boolean corrupt = !isRequestChecksumCorrect(exchange);
        final DivolteIdentifier partyId = queryParamFromExchange(exchange, PARTY_ID_QUERY_PARAM).flatMap(DivolteIdentifier::tryParse).orElseThrow(IncompleteRequestException::new);
        final DivolteIdentifier sessionId = queryParamFromExchange(exchange, SESSION_ID_QUERY_PARAM).flatMap(DivolteIdentifier::tryParse).orElseThrow(IncompleteRequestException::new);
        final String pageViewId = queryParamFromExchange(exchange, PAGE_VIEW_ID_QUERY_PARAM).orElseThrow(IncompleteRequestException::new);
        final String eventId = queryParamFromExchange(exchange, EVENT_ID_QUERY_PARAM).orElseThrow(IncompleteRequestException::new);
        final boolean isNewPartyId = queryParamFromExchange(exchange, NEW_PARTY_ID_QUERY_PARAM).map(TRUE_STRING::equals).orElseThrow(IncompleteRequestException::new);
        final boolean isFirstInSession = queryParamFromExchange(exchange, FIRST_IN_SESSION_QUERY_PARAM).map(TRUE_STRING::equals).orElseThrow(IncompleteRequestException::new);
        final long clientTimeStamp = queryParamFromExchange(exchange, CLIENT_TIMESTAMP_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Long).orElseThrow(IncompleteRequestException::new);

        final long requestTime = System.currentTimeMillis();
        final DivolteEvent eventData = buildBrowserEventData(corrupt, partyId, sessionId, pageViewId, eventId,
                                                             requestTime, clientTimeStamp - requestTime,
                                                             isNewPartyId, isFirstInSession, exchange);

        exchange.putAttachment(DIVOLTE_EVENT_KEY, eventData);

        logger.debug("Enqueuing event (client generated cookies): {}/{}/{}/{}", partyId, sessionId, pageViewId, eventId);
        processingPool.enqueueIncomingExchangeForProcessing(partyId, exchange);
    }

    static DivolteEvent buildBrowserEventData(final boolean corruptEvent,
                                              final DivolteIdentifier partyCookie,
                                              final DivolteIdentifier sessionCookie,
                                              final String pageViewId,
                                              final String eventId,
                                              final long requestStartTime,
                                              final long clientUtcOffset,
                                              final boolean newPartyId,
                                              final boolean firstInSession,
                                              final HttpServerExchange exchange) {
        return new DivolteEvent(corruptEvent,
                                partyCookie,
                                sessionCookie,
                                eventId,
                                EVENT_SOURCE_NAME,
                                requestStartTime,
                                clientUtcOffset,
                                newPartyId,
                                firstInSession,
                                queryParamFromExchange(exchange, EVENT_TYPE_QUERY_PARAM),
                                () -> queryParamFromExchange(exchange, EVENT_PARAMETERS_QUERY_PARAM)
                                    .map(encodedParameters -> {
                                        try {
                                            return EVENT_PARAMETERS_READER.readTree(encodedParameters);
                                        } catch (final IOException e) {
                                            if (logger.isDebugEnabled()) {
                                                logger.debug("Could not parse custom event parameters: " + encodedParameters, e);
                                            }
                                            return null;
                                        }
                                    }),
                                Optional.of(new DivolteEvent.BrowserEventData(pageViewId,
                                                                          queryParamFromExchange(exchange, LOCATION_QUERY_PARAM),
                                                                          queryParamFromExchange(exchange, REFERER_QUERY_PARAM),
                                                                          queryParamFromExchange(exchange, VIEWPORT_PIXEL_WIDTH_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Int),
                                                                          queryParamFromExchange(exchange, VIEWPORT_PIXEL_HEIGHT_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Int),
                                                                          queryParamFromExchange(exchange, SCREEN_PIXEL_WIDTH_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Int),
                                                                          queryParamFromExchange(exchange, SCREEN_PIXEL_HEIGHT_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Int),
                                                                          queryParamFromExchange(exchange, DEVICE_PIXEL_RATIO_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Int)
                                                                          ))
                                );
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
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
    static Long tryParseBase36Long(String input) {
        try {
            return Long.parseLong(input, 36);
        } catch(NumberFormatException nfe) {
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
