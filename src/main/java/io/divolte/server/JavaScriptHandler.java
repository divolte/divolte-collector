package io.divolte.server;

import io.divolte.server.js.JavaScriptResource;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request handler for a JavaScript resource, with caching support.
 */
@ParametersAreNonnullByDefault
final class JavaScriptHandler implements HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final JavaScriptResource resource;

    public JavaScriptHandler(final JavaScriptResource resource) {
        this.resource = Objects.requireNonNull(resource);
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Requested received for {} from {}",
                         resource.getResourceName(), exchange.getSourceAddress().getHostString());
        }
        final String eTag = resource.getETag();
        final HeaderMap responseHeaders = exchange.getResponseHeaders();
        responseHeaders.put(Headers.CACHE_CONTROL, "public");
        responseHeaders.put(Headers.ETAG, eTag);
        final HeaderValues ifMatch = exchange.getRequestHeaders().get(Headers.IF_NONE_MATCH);
        if (isETagMatch(ifMatch, eTag)) {
            exchange.setResponseCode(StatusCodes.NOT_MODIFIED);
            exchange.endExchange();
        } else {
            final ByteBuffer entityBody = resource.getEntityBody();
            responseHeaders.put(Headers.CONTENT_TYPE, "application/javascript");
            exchange.getResponseSender().send(entityBody);
        }
    }

    private static boolean isETagMatch(@Nullable final HeaderValues headerValues, final String eTag) {
        // Warning: Rarely return to short-circuit logic.
        if (null != headerValues) {
            for (final String headerValue : headerValues) {
                if (eTag.equals(headerValue)) {
                    return true;
                } else if ("*".equals(headerValue)) {
                    return true;
                }
            }
        }
        return false;
    }
}
