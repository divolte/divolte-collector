package io.divolte.server;

import io.divolte.server.js.GzippableHttpBody;
import io.divolte.server.js.HttpBody;
import io.divolte.server.js.JavaScriptResource;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
 * Request handler for a JavaScript resource, with caching support.
 */
@ParametersAreNonnullByDefault
final class JavaScriptHandler implements HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private static final Splitter HEADER_SPLITTER =
            Splitter.on(',').trimResults().omitEmptyStrings();

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
        // Start with headers that we always set the same way.
        final HeaderMap responseHeaders = exchange.getResponseHeaders();
        responseHeaders.put(Headers.CACHE_CONTROL, "public");

        // Figure out if we possibly need to deal with a compressed response,
        // based on client capability.
        final GzippableHttpBody uncompressedBody = resource.getEntityBody();
        final Optional<HttpBody> gzippedBody = uncompressedBody.getGzippedBody();
        final HttpBody bodyToSend;
        if (gzippedBody.isPresent()) {
            /*
             * Compressed responses can use Content-Encoding and/or Transfer-Encoding.
             * The semantics differ slightly, but it is suffice to say that most user
             * agents don't advertise their Transfer-Encoding support.
             * So for now we only support the Content-Encoding mechanism.
             * Some other notes:
             *  - Some clients implement 'deflate' incorrectly. Hence we only support 'gzip',
             *    despite it having slightly more overhead.
             *  - We don't use Undertow's built-in compression support because we've
             *    pre-calculated the compressed response and expect to serve it up
             *    repeatedly, instead of calculating it on-the-fly for every request.
             */
            responseHeaders.put(Headers.VARY, Headers.ACCEPT_ENCODING_STRING);
            final HeaderValues acceptEncoding =
                    exchange.getRequestHeaders().get(Headers.ACCEPT_ENCODING);
            if (null != acceptEncoding &&
                    acceptEncoding.stream()
                                  .anyMatch((header) -> Iterables.contains(HEADER_SPLITTER.split(header), "gzip"))) {
                responseHeaders.put(Headers.CONTENT_ENCODING, "gzip");
                bodyToSend = gzippedBody.get();
            } else {
                bodyToSend = uncompressedBody;
            }
        } else {
            bodyToSend = uncompressedBody;
        }

        // Now we know which version of the entity is visible to this user-agent,
        // figure out if the client already has the current version or not.
        final String eTag = bodyToSend.getETag();
        responseHeaders.put(Headers.ETAG, eTag);
        final HeaderValues ifNoneMatch = exchange.getRequestHeaders().get(Headers.IF_NONE_MATCH);
        if (isETagMatch(ifNoneMatch, eTag)) {
            exchange.setResponseCode(StatusCodes.NOT_MODIFIED);
            exchange.endExchange();
        } else {
            final ByteBuffer entityBody = bodyToSend.getBody();
            responseHeaders.put(Headers.CONTENT_TYPE, "application/javascript");
            exchange.getResponseSender().send(entityBody);
        }
    }

    private static boolean isETagMatch(@Nullable final HeaderValues headerValues, final String eTag) {
        // Warning: Early return to short-circuit logic.
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
