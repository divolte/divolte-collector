package io.divolte.server;

import io.divolte.server.CookieValues.CookieValue;
import io.undertow.server.HttpServerExchange;

import java.net.InetSocketAddress;
import java.util.Deque;
import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

@ParametersAreNonnullByDefault
final class ClientSideCookieEventHandler extends BaseEventHandler {
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
        final long clientTimeStamp = queryParamFromExchange(exchange, CLIENT_TIMESTAMP_QUERY_PARAM).map(Longs::tryParse).orElseThrow(IncompleteRequestException::new);

        final long requestTime = System.currentTimeMillis();
        exchange.putAttachment(REQUEST_START_TIME_KEY, requestTime);
        exchange.putAttachment(COOKIE_UTC_OFFSET, clientTimeStamp - requestTime);

        exchange.putAttachment(PARTY_COOKIE_KEY, partyId);
        exchange.putAttachment(SESSION_COOKIE_KEY, sessionId);
        exchange.putAttachment(PAGE_VIEW_ID_KEY, pageViewId);

        logger.debug("Enqueuing event (client generated cookies): {}/{}/{}", partyId, sessionId, pageViewId);
        processingPool.enqueueIncomingExchangeForProcessing(partyId, exchange);
    }

    private Optional<String> queryParamFromExchange(final HttpServerExchange exchange, final String param) {
        return Optional.ofNullable(exchange.getQueryParameters().get(param)).map(Deque::getFirst);
    }

    private static class IncompleteRequestException extends Exception {
        private static final long serialVersionUID = 3991442606210410941L;
    }
}
