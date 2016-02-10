package io.divolte.server;

import io.undertow.server.HttpServerExchange;

public abstract class UndertowEvent {
    public final long requestTime;
    public final HttpServerExchange exchange;
    public final DivolteIdentifier partyId;

    public UndertowEvent(final long requestTime, final HttpServerExchange exchange, final DivolteIdentifier partyId) {
        this.requestTime = requestTime;
        this.exchange = exchange;
        this.partyId = partyId;
    }

    public abstract DivolteEvent parseRequest() throws IncompleteRequestException;
}
