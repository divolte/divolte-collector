package io.divolte.server;

import static io.divolte.server.HttpSource.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import io.divolte.server.processing.Item;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.StatusCodes;

public class JsonEventHandler implements HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(JsonEventHandler.class);


    private final IncomingRequestProcessingPool processingPool;
    private final int sourceIndex;
    private final String partyIdParameter;

    public JsonEventHandler(
            final IncomingRequestProcessingPool processingPool,
            final int sourceIndex,
            final String partyIdParameter) {
        this.processingPool = processingPool;
        this.sourceIndex = sourceIndex;
        this.partyIdParameter = partyIdParameter;
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) {
        captureAndPersistSourceAddress(exchange);

        try {
            // TODO: capture request body similar to RequestBufferingHandler
            // XXX: This is seriously broken!
            final ByteBuffer buffer = ByteBuffer.allocate((int) exchange.getRequestContentLength());
            exchange.getRequestChannel().read(buffer);
            buffer.flip();

            final byte[] body = buffer.array();
            logEvent(exchange, body);
        } catch (final IncompleteRequestException e) {
            // improper request, could be anything
            logger.warn("Improper request received from {}.", Optional.ofNullable(exchange.getSourceAddress()).map(InetSocketAddress::getHostString).orElse("<UNKNOWN HOST>"));
        } catch (final IOException e) {
            // Could not read request body
            logger.warn("Error while reading request body received from {}.", Optional.ofNullable(exchange.getSourceAddress()).map(InetSocketAddress::getHostString).orElse("<UNKNOWN HOST>"));
        } finally {
            exchange.setStatusCode(StatusCodes.NO_CONTENT);
            exchange.endExchange();
        }
    }

    private void logEvent(final HttpServerExchange exchange, final byte[] body) throws IncompleteRequestException {
        final DivolteIdentifier partyId = queryParamFromExchange(exchange, partyIdParameter).flatMap(DivolteIdentifier::tryParse).orElseThrow(IncompleteRequestException::new);
        final UndertowEvent event = new JsonUndertowEvent(System.currentTimeMillis(), exchange, partyId, body);
        processingPool.enqueue(Item.of(sourceIndex, partyId.value, event));
    }

    private static final class JsonUndertowEvent extends UndertowEvent {
        private static final ObjectMapper OBJECT_MAPPER;
        static {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.setPropertyNamingStrategy(
                    new PropertyNamingStrategy.LowerCaseWithUnderscoresStrategy() // snake_casing
                    );
            mapper.registerModules(
                    new ParameterNamesModule() // Support JDK8 parameter name discovery
                    );

            OBJECT_MAPPER = mapper;
        }

        private final byte[] requestBody;

        /*
         * PMD erroneously flags that the byte array passed to this constructor
         * is mutable from other code as this instance lives on. In reality,
         * this is the only scope where it is available after creation.
         */
        @SuppressWarnings("PMD.ArrayIsStoredDirectly")
        private JsonUndertowEvent(
                final long requestTime,
                final HttpServerExchange exchange,
                final DivolteIdentifier partyId,
                final byte[] requestBody) throws IncompleteRequestException {
            super(requestTime, exchange, partyId);
            this.requestBody = requestBody;
        }

        @Override
        public DivolteEvent parseRequest() throws IncompleteRequestException {
            final EventContainer container;
            try {
                container = OBJECT_MAPPER.readValue(requestBody, EventContainer.class);
            } catch(final JsonMappingException me) {
                logger.info("JSON mapping failed for request: {}", me.getMessage());
                throw new IncompleteRequestException();
            } catch (final IOException e) {
                // XXX: Is this corrupt or incomplete???
                logger.warn("Parsing failed for request.", e);
                throw new IncompleteRequestException();
            }

            /*
             * XXX: A JSON event cannot be corrupt at the moment. Either the request is complete and eveything works,
             * or the request is incomplete and we drop it as we cannot parse enough to provide the mapping with a
             * partial event.
             */
            final boolean corrupt = false;

            /*
             * XXX: Currently, we allow the event type to be absent. With the browser end point, this
             * doesn't happen in practice, though. Should we perhaps require it to be present for a
             * valid request?
             */
            final DivolteEvent event = DivolteEvent.createJsonEvent(
                    exchange, corrupt, partyId, DivolteIdentifier.tryParse(container.sessionId).orElseThrow(IncompleteRequestException::new),
                    container.eventId, JsonSource.EVENT_SOURCE_NAME, requestTime, container.clientTimestamp - requestTime, container.isNewParty,
                    container.isNewSession, Optional.of(container.eventType), () -> Optional.ofNullable(container.parameters),
                    DivolteEvent.JsonEventData.EMPTY);

            return event;
        }

        final static class EventContainer {
            @JsonProperty(required=true) public final String eventType;
            @JsonProperty(required=true) public final String sessionId;
            @JsonProperty(required=true) public final String eventId;
            @JsonProperty(required=true) public final boolean isNewParty;
            @JsonProperty(required=true) public final boolean isNewSession;
            @JsonProperty(required=true) public final long clientTimestamp;
            public final JsonNode parameters;

            @JsonCreator
            public EventContainer(
                    final String eventType, final String sessionId, final String eventId, final boolean isNewParty,
                    final boolean isNewSession, final long clientTimestamp, final JsonNode parameters) {
                this.eventType = eventType;
                this.sessionId = sessionId;
                this.eventId = eventId;
                this.isNewParty = isNewParty;
                this.isNewSession = isNewSession;
                this.clientTimestamp = clientTimestamp;
                this.parameters = parameters;
            }
        }
    }
}
