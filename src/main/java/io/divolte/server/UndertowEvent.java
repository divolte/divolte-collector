package io.divolte.server;

import io.undertow.server.HttpServerExchange;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Instant;
import java.util.Objects;

@ParametersAreNonnullByDefault
public abstract class UndertowEvent {
    public final Instant requestTime;
    public final HttpServerExchange exchange;
    public final DivolteIdentifier partyId;

    public UndertowEvent(final Instant requestTime, final HttpServerExchange exchange, final DivolteIdentifier partyId) {
        this.requestTime = Objects.requireNonNull(requestTime);
        this.exchange = Objects.requireNonNull(exchange);
        this.partyId = Objects.requireNonNull(partyId);
    }

    public abstract DivolteEvent parseRequest() throws IncompleteRequestException;
}
