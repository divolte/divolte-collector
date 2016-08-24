package io.divolte.server;

import static io.divolte.server.HttpSource.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import io.divolte.server.processing.Item;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.StatusCodes;

@ParametersAreNonnullByDefault
public class JsonEventHandler implements HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(JsonEventHandler.class);

    private final IncomingRequestProcessingPool processingPool;
    private final int sourceIndex;
    private final String partyIdParameter;

    private final AsyncRequestBodyReceiver receiver;

    public JsonEventHandler(final IncomingRequestProcessingPool processingPool,
                            final int sourceIndex,
                            final String partyIdParameter,
                            final int maximumBodySize) {
        this.processingPool   = Objects.requireNonNull(processingPool);
        this.sourceIndex      = sourceIndex;
        this.partyIdParameter = Objects.requireNonNull(partyIdParameter);

        receiver = new AsyncRequestBodyReceiver(maximumBodySize);
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) {
        captureAndPersistSourceAddress(exchange);

        receiver.receive((body,length) -> {
            try {
                if (0 < length) {
                    logEvent(exchange, body);
                    exchange.setStatusCode(StatusCodes.NO_CONTENT);
                } else {
                    // Empty body; bad by definition.
                    exchange.setStatusCode(StatusCodes.BAD_REQUEST);
                }
            } catch (final IncompleteRequestException e) {
                // Improper request, could be anything.
                exchange.setStatusCode(StatusCodes.BAD_REQUEST);
                logger.warn("Improper request received from {}.",
                            Optional.ofNullable(exchange.getSourceAddress())
                                    .map(InetSocketAddress::getHostString)
                                    .orElse("<UNKNOWN HOST>"));
            } finally {
                exchange.endExchange();
            }
        }, exchange);
    }


    private void logEvent(final HttpServerExchange exchange, final InputStream body) throws IncompleteRequestException {
        final DivolteIdentifier partyId = queryParamFromExchange(exchange, partyIdParameter).flatMap(DivolteIdentifier::tryParse)
                                                                                            .orElseThrow(IncompleteRequestException::new);
        final UndertowEvent event = new JsonUndertowEvent(Instant.now(), exchange, partyId, body);
        processingPool.enqueue(Item.of(sourceIndex, partyId.value, event));
    }

    @ParametersAreNonnullByDefault
    private static final class JsonUndertowEvent extends UndertowEvent {
        private static final ObjectMapper OBJECT_MAPPER;
        static {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
            // Support JDK8 parameter name discovery
            mapper.registerModules(new ParameterNamesModule());
            mapper.registerModule(new Jdk8Module());
            OBJECT_MAPPER = mapper;
        }

        private final InputStream requestBody;

        private JsonUndertowEvent(
                final Instant requestTime,
                final HttpServerExchange exchange,
                final DivolteIdentifier partyId,
                final InputStream requestBody) throws IncompleteRequestException {
            super(requestTime, exchange, partyId);
            this.requestBody = Objects.requireNonNull(requestBody);
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
                // This indicates we couldn't parse the data. Corrupt or incomplete,
                // we can't proceed because mapping is all-or-nothing and we don't
                // even have a partial object.
                logger.warn("Parsing failed for request.", e);
                throw new IncompleteRequestException();
            }

            /*
             * Parse the client provided timestamp as ISO offsetted date/time. We use the ofEpochSecond creator to
             * obtain an Instant, as the Instant#from(TemporalAccessor) performs some additional checks unnecessary
             * in our case.
             */
            final TemporalAccessor parsed = DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(container.clientTimestampIso);
            final Instant clientTime = Instant.from(parsed);
            final DivolteEvent event = DivolteEvent.createJsonEvent(
                    exchange, partyId,
                    DivolteIdentifier.tryParse(container.sessionId).orElseThrow(IncompleteRequestException::new),
                    container.eventId, JsonSource.EVENT_SOURCE_NAME, requestTime, clientTime,
                    container.isNewParty, container.isNewSession, container.eventType,
                    () -> container.parameters, // Note that it's possible to send a JSON event without parameters
                    DivolteEvent.JsonEventData.EMPTY);

            return event;
        }

        @ParametersAreNonnullByDefault
        private final static class EventContainer {
            public final Optional<String> eventType;
            @JsonProperty(required=true) public final String sessionId;
            @JsonProperty(required=true) public final String eventId;
            @JsonProperty(required=true) public final boolean isNewParty;
            @JsonProperty(required=true) public final boolean isNewSession;
            @JsonProperty(required=true) public final String clientTimestampIso;
            public final Optional<JsonNode> parameters;

            @JsonCreator
            public EventContainer(
                    final Optional<String> eventType, final String sessionId, final String eventId, final boolean isNewParty,
                    final boolean isNewSession, final String clientTimestampIso, final Optional<JsonNode> parameters) {
                this.eventType          = Objects.requireNonNull(eventType);
                this.sessionId          = Objects.requireNonNull(sessionId);
                this.eventId            = Objects.requireNonNull(eventId);
                this.isNewParty         = Objects.requireNonNull(isNewParty);
                this.isNewSession       = Objects.requireNonNull(isNewSession);
                this.clientTimestampIso = Objects.requireNonNull(clientTimestampIso);
                this.parameters         = Objects.requireNonNull(parameters);
            }
        }
    }
}
