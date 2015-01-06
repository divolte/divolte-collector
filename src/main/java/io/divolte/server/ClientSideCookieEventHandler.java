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

import static io.divolte.server.IncomingRequestProcessor.*;
import io.divolte.server.CookieValues.CookieValue;
import io.divolte.server.recordmapping.ConfigRecordMapper;
import io.undertow.server.HttpServerExchange;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

@ParametersAreNonnullByDefault
public final class ClientSideCookieEventHandler extends BaseEventHandler {
    private static final String TRUE_STRING = "t";
    private final static Logger logger = LoggerFactory.getLogger(ClientSideCookieEventHandler.class);

    public final static String PARTY_ID_QUERY_PARAM = "p";
    public final static String NEW_PARTY_ID_QUERY_PARAM = "n";
    public final static String SESSION_ID_QUERY_PARAM = "s";
    public final static String FIRST_IN_SESSION_QUERY_PARAM = "f";
    public final static String PAGE_VIEW_ID_QUERY_PARAM = "v";
    public final static String EVENT_ID_QUERY_PARAM = "e";
    public final static String EVENT_TYPE_QUERY_PARAM = "t";
    public final static String CLIENT_TIMESTAMP_QUERY_PARAM = "c"; // chronos
    public final static String LOCATION_QUERY_PARAM = "l";
    public final static String REFERER_QUERY_PARAM = "r";
    public final static String VIEWPORT_PIXEL_WIDTH_QUERY_PARAM = "w";
    public final static String VIEWPORT_PIXEL_HEIGHT_QUERY_PARAM = "h";
    public final static String SCREEN_PIXEL_WIDTH_QUERY_PARAM = "i";
    public final static String SCREEN_PIXEL_HEIGHT_QUERY_PARAM = "j";
    public final static String DEVICE_PIXEL_RATIO_QUERY_PARAM = "k";
    public final static String CHECKSUM_QUERY_PARAM = "x";

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
        exchange.putAttachment(CORRUPT_EVENT_KEY, corrupt);

        final CookieValue partyId = queryParamFromExchange(exchange, PARTY_ID_QUERY_PARAM).flatMap(CookieValues::tryParse).orElseThrow(IncompleteRequestException::new);
        final CookieValue sessionId = queryParamFromExchange(exchange, SESSION_ID_QUERY_PARAM).flatMap(CookieValues::tryParse).orElseThrow(IncompleteRequestException::new);
        final String pageViewId = queryParamFromExchange(exchange, PAGE_VIEW_ID_QUERY_PARAM).orElseThrow(IncompleteRequestException::new);
        final String eventId = queryParamFromExchange(exchange, EVENT_ID_QUERY_PARAM).orElseThrow(IncompleteRequestException::new);
        /*final boolean isNewPartyId = */queryParamFromExchange(exchange, NEW_PARTY_ID_QUERY_PARAM).map(TRUE_STRING::equals).orElseThrow(IncompleteRequestException::new);
        final boolean isFirstInSession = queryParamFromExchange(exchange, FIRST_IN_SESSION_QUERY_PARAM).map(TRUE_STRING::equals).orElseThrow(IncompleteRequestException::new);
        final long clientTimeStamp = queryParamFromExchange(exchange, CLIENT_TIMESTAMP_QUERY_PARAM).map(ClientSideCookieEventHandler::tryParseBase36Long).orElseThrow(IncompleteRequestException::new);

        final long requestTime = System.currentTimeMillis();
        exchange.putAttachment(REQUEST_START_TIME_KEY, requestTime);
        exchange.putAttachment(COOKIE_UTC_OFFSET_KEY, clientTimeStamp - requestTime);

        exchange.putAttachment(PARTY_COOKIE_KEY, partyId);
        exchange.putAttachment(SESSION_COOKIE_KEY, sessionId);
        exchange.putAttachment(PAGE_VIEW_ID_KEY, pageViewId);
        exchange.putAttachment(EVENT_ID_KEY, eventId);
        exchange.putAttachment(FIRST_IN_SESSION_KEY, isFirstInSession);

        exchange.putAttachment(LOCATION_KEY, queryParamFromExchange(exchange, LOCATION_QUERY_PARAM));
        exchange.putAttachment(REFERER_KEY, queryParamFromExchange(exchange, REFERER_QUERY_PARAM));
        exchange.putAttachment(VIEWPORT_PIXEL_WIDTH_KEY, queryParamFromExchange(exchange, VIEWPORT_PIXEL_WIDTH_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int));
        exchange.putAttachment(VIEWPORT_PIXEL_HEIGHT_KEY, queryParamFromExchange(exchange, VIEWPORT_PIXEL_HEIGHT_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int));
        exchange.putAttachment(SCREEN_PIXEL_WIDTH_KEY, queryParamFromExchange(exchange, SCREEN_PIXEL_WIDTH_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int));
        exchange.putAttachment(SCREEN_PIXEL_HEIGHT_KEY, queryParamFromExchange(exchange, SCREEN_PIXEL_HEIGHT_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int));
        exchange.putAttachment(DEVICE_PIXEL_RATIO_KEY, queryParamFromExchange(exchange, DEVICE_PIXEL_RATIO_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int));

        exchange.putAttachment(EVENT_TYPE_KEY, queryParamFromExchange(exchange, EVENT_TYPE_QUERY_PARAM));
        exchange.putAttachment(EVENT_PARAM_PRODUCER_KEY, (name) -> queryParamFromExchange(exchange, EVENT_TYPE_QUERY_PARAM + "." + name));

        logger.debug("Enqueuing event (client generated cookies): {}/{}/{}/{}", partyId, sessionId, pageViewId, eventId);
        processingPool.enqueueIncomingExchangeForProcessing(partyId, exchange);
    }

    private static Optional<String> queryParamFromExchange(final HttpServerExchange exchange, final String param) {
        return Optional.ofNullable(exchange.getQueryParameters().get(param)).map(Deque::getFirst);
    }

    static Long tryParseBase36Long(String input) {
        try {
            return Long.parseLong(input, 36);
        } catch(NumberFormatException nfe) {
            return null;
        }
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
}
