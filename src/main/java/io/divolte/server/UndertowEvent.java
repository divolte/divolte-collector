package io.divolte.server;

import io.undertow.server.HttpServerExchange;

import java.time.Instant;
public abstract class UndertowEvent {
    public final Instant requestTime;
    public final HttpServerExchange exchange;
    public final DivolteIdentifier partyId;

    public UndertowEvent(final Instant requestTime, final HttpServerExchange exchange, final DivolteIdentifier partyId) {
        this.requestTime = requestTime;
        this.exchange = exchange;
        this.partyId = partyId;
    }

    public abstract DivolteEvent parseRequest() throws IncompleteRequestException;
}
