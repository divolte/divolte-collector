package io.divolte.server;

import static io.divolte.server.HttpSource.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import io.divolte.server.ClientSideCookieEventHandler.IncompleteRequestException;
import io.divolte.server.processing.Item;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.StatusCodes;

public class JsonEventHandler implements HttpHandler {
    private static final String TRUE_STRING = "t";

    private final IncomingRequestProcessingPool processingPool;
    private final int sourceIndex;
    private final String eventTypeParameter;
    private final String partyIdParameter;
    private final String sessionIdParameter;
    private final String eventIdParameter;
    private final String newPartyParameter;
    private final String newSessionParameter;
    private final String timeParameter;

    private static final ObjectReader EVENT_PARAMETERS_READER = new ObjectMapper().reader();

    public JsonEventHandler(
            final IncomingRequestProcessingPool processingPool,
            final int sourceIndex,
            final String eventTypeParameter,
            final String partyIdParameter,
            final String sessionIdParameter,
            final String eventIdParameter,
            final String newPartyParameter,
            final String newSessionParameter,
            final String timeParameter) {
        this.processingPool = processingPool;
        this.sourceIndex = sourceIndex;
        this.eventTypeParameter = eventTypeParameter;
        this.partyIdParameter = partyIdParameter;
        this.sessionIdParameter = sessionIdParameter;
        this.eventIdParameter = eventIdParameter;
        this.newPartyParameter = newPartyParameter;
        this.newSessionParameter = newSessionParameter;
        this.timeParameter = timeParameter;
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        captureAndPersistSourceAddress(exchange);

        /*
         * JSON events cannot be corrupt. In case of missing parameters, we discard whatsoever.
         * In case of bad JSON, this will result in a mapping error and not be discovered on
         * the HTTP thread.
         *
         * We could technically flag a corrupt event if there is a party ID, but other fields are
         * missing, but there seems little benefit in having these in the event stream.
         */
        final boolean corrupt = false;

        /*
         * XXX: Currently, we allow the event type to be absent. With the browser end point, this
         * doesn't happen in practice, though. Should we perhaps require it to be present for a
         * valid request?
         */
        final Optional<String> eventType = queryParamFromExchange(exchange, eventTypeParameter);

        final DivolteIdentifier partyId = queryParamFromExchange(exchange, partyIdParameter).flatMap(DivolteIdentifier::tryParse).orElseThrow(IncompleteRequestException::new);
        final DivolteIdentifier sessionId = queryParamFromExchange(exchange, sessionIdParameter).flatMap(DivolteIdentifier::tryParse).orElseThrow(IncompleteRequestException::new);
        final String eventId = queryParamFromExchange(exchange, eventIdParameter).orElseThrow(IncompleteRequestException::new);
        final boolean isNewPartyId = queryParamFromExchange(exchange, newPartyParameter).map(TRUE_STRING::equals).orElseThrow(IncompleteRequestException::new);
        final boolean isFirstInSession = queryParamFromExchange(exchange, newSessionParameter).map(TRUE_STRING::equals).orElseThrow(IncompleteRequestException::new);
        final long clientTimeStamp = queryParamFromExchange(exchange, timeParameter).map(ClientSideCookieEventHandler::tryParseBase36Long).orElseThrow(IncompleteRequestException::new);

        final long requestTime = System.currentTimeMillis();

        final DivolteEvent event = DivolteEvent.createJsonEvent(
                exchange, corrupt, partyId, sessionId, eventId, JsonSource.EVENT_SOURCE_NAME, requestTime,
                clientTimeStamp - requestTime, isNewPartyId, isFirstInSession,
                eventType,
                eventParametersSupplier(exchange),
                DivolteEvent.JsonEventData.EMPTY);

        exchange.setStatusCode(StatusCodes.NO_CONTENT);
        exchange.endExchange();

        processingPool.enqueue(Item.of(sourceIndex, event.partyId.value, event));
    }

    // TODO: Temporary solution!!!
    private Supplier<Optional<JsonNode>> eventParametersSupplier(final HttpServerExchange exchange) {
        if (exchange.getRequestContentLength() == 0) {
            return () -> Optional.empty();
        } else {
            // TODO: broken; model this after RequestBufferingHandler from Undertow itself
            final ByteBuffer buffer = ByteBuffer.allocate((int) exchange.getRequestContentLength());
            try {
                exchange.getRequestChannel().read(buffer);
                buffer.flip();
                final byte[] body = buffer.array();
                return () -> {
                    try {
                        return Optional.of(EVENT_PARAMETERS_READER.readTree(new ByteArrayInputStream(body)));
                    } catch (final Exception e) {
                        return Optional.empty();
                    }
                };
            } catch (final IOException e) {
                return () -> Optional.empty();
            }
        }
    }
}
