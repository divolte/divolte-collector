package io.divolte.server;

import io.divolte.server.CookieValues.CookieValue;
import io.undertow.server.HttpServerExchange;

import java.net.InetSocketAddress;
import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ParametersAreNonnullByDefault
final class ClientSideCookieEventHandler extends BaseEventHandler {
    private static final String TRUE_STRING = "t";
    private final static Logger logger = LoggerFactory.getLogger(ClientSideCookieEventHandler.class);

    public ClientSideCookieEventHandler(final IncomingRequestProcessingPool pool) {
        super(pool);
    }

    @Override
    protected void doHandleEventRequest(HttpServerExchange exchange) throws Exception {
        try {
            handleRequestIfComplete(exchange);
        } catch(IncompleteRequestException ire) {
            // improper request, could be anything
            logger.warn("Improper request received from {}.", Optional.ofNullable(exchange.getSourceAddress()).map(InetSocketAddress::getHostString).orElse("<UNKNOWN HOST>"));
        } finally {
            serveImage(exchange);
        }
    }

    private void handleRequestIfComplete(final HttpServerExchange exchange) throws IncompleteRequestException {
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

        logger.debug("Enqueuing event (client generated cookies): {}/{}/{}", partyId, sessionId, pageViewId);
        processingPool.enqueueIncomingExchangeForProcessing(partyId, exchange);
    }

    private static Long tryParseBase36Long(String input) {
        try {
            return Long.parseLong(input, 36);
        } catch(NumberFormatException nfe) {
            return null;
        }
    }
}
