package io.divolte.server;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.StatusCodes;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

@ParametersAreNonnullByDefault
public class AllowedMethodsHandler implements HttpHandler {

    private final ImmutableSet<HttpString> allowedMethods;
    private final HttpHandler next;
    private final String allowedMethodHeader;

    public AllowedMethodsHandler(final HttpHandler next, final ImmutableSet<HttpString> allowedMethods) {
        this.allowedMethods = Objects.requireNonNull(allowedMethods);
        this.next = Objects.requireNonNull(next);
        this.allowedMethodHeader = Joiner.on(", ").join(allowedMethods);
    }

    public AllowedMethodsHandler(final HttpHandler next, final HttpString... allowedMethods) {
        this(next, ImmutableSet.copyOf(allowedMethods));
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        final HttpString requestMethod = exchange.getRequestMethod();
        if (allowedMethods.contains(requestMethod)) {
            next.handleRequest(exchange);
        } else {
            exchange.setResponseCode(StatusCodes.METHOD_NOT_ALLOWED);
            exchange.getResponseHeaders()
                    .put(Headers.ALLOW, allowedMethodHeader)
                    .put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
            exchange.getResponseSender()
                    .send("HTTP method" + requestMethod + " + not allowed.", StandardCharsets.UTF_8);
            exchange.endExchange();
        }
    }
}
