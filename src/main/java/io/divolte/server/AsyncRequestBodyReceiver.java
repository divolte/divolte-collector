package io.divolte.server;

import com.google.common.primitives.Ints;
import io.undertow.UndertowMessages;
import io.undertow.io.IoCallback;
import io.undertow.io.Sender;
import io.undertow.server.Connectors;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.protocol.http.HttpContinue;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.channels.StreamSourceChannel;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

@ParametersAreNonnullByDefault
public class AsyncRequestBodyReceiver {
    private static final Logger logger = LoggerFactory.getLogger(AsyncRequestBodyReceiver.class);

    private static final InputStream EMPTY_INPUT_STREAM = new InputStream() {
        @Override
        public int read() throws IOException {
            return -1;
        }
    };

    private final AtomicInteger preallocateChunks = new AtomicInteger(1);
    private final int maximumChunks;

    public AsyncRequestBodyReceiver(final int requestedMaxBufferSize) {
        /*
         * Convert requested buffer size into the number of slots.
         * We always need at least one slot, and the actual buffer
         * may be larger than requested because we always use a slot
         * completely.
         */
        final int requestedSlots = calculateChunks(requestedMaxBufferSize);
        maximumChunks = Math.max(requestedSlots, 1);
        logger.debug("Configuring to use a maximum of {} buffer chunks.", maximumChunks);
    }

    private static int calculateChunks(final int length) {
        // Should be faster than using Math.ceil().
        return (length - 1) / ChunkyByteBuffer.CHUNK_SIZE + 1;
    }

    private static void rejectLargeRequest(final HttpServerExchange exchange) {
        exchange.setStatusCode(StatusCodes.REQUEST_ENTITY_TOO_LARGE)
                .setPersistent(false)
                .endExchange();
    }

    public void receive(final BiConsumer<InputStream, Integer> callback, final HttpServerExchange exchange) {
        Objects.requireNonNull(callback);
        if (logger.isDebugEnabled()) {
            logger.debug("Pre-allocating buffer with {} chunks.", preallocateChunks.get());
        }

        if (exchange.isRequestComplete()) {
            logger.debug("Request already completed; zero length content body.");
            callback.accept(EMPTY_INPUT_STREAM, 0);
            return;
        }

        // Determine the number of buffer-slots to use, trusting a well-formed content-length
        // header if that's present.
        final Optional<Integer> contentLength =
                Optional.ofNullable(exchange.getRequestHeaders().getFirst(Headers.CONTENT_LENGTH))
                        .map(Ints::tryParse);
        if (logger.isDebugEnabled()) {
            logger.debug("Content length claimed by request: {}", contentLength);
        }
        final int provisionChunks = contentLength.map(AsyncRequestBodyReceiver::calculateChunks)
                                                 .orElseGet(preallocateChunks::get);
        if (provisionChunks > maximumChunks) {
            logger.info("Rejecting request; anticipated content length ({}) will exceed exceed maximum allowed {})",
                         contentLength, maximumChunks * ChunkyByteBuffer.CHUNK_SIZE);
            rejectLargeRequest(exchange);
            return;
        }

        // Some clients will pause before sending the body, waiting for a '100 Continue' response.
        // If we've reached this point we're always going to accept the body, so continuing is fine.
        if (HttpContinue.requiresContinueResponse(exchange)) {
            logger.debug("Request requires permission to proceed; allowing to continue.");
            HttpContinue.sendContinueResponse(exchange, new IoCallback() {
                @Override
                public void onComplete(final HttpServerExchange exchange, final Sender sender) {
                    receive(callback, exchange, contentLength, provisionChunks);
                }
                @Override
                public void onException(final HttpServerExchange exchange, final Sender sender, final IOException exception) {
                    logger.error("Error allowing request to continue.");
                    exchange.endExchange();
                }
            });
        } else {
            receive(callback, exchange, contentLength, provisionChunks);
        }
    }

    private static String getPeerHost(final HttpServerExchange exchange) {
        return Optional.ofNullable(exchange.getSourceAddress())
            .map(InetSocketAddress::getHostString)
            .orElse("<UNKNOWN HOST>");
    }

    private void receive(final BiConsumer<InputStream, Integer> callback,
                         final HttpServerExchange exchange,
                         final Optional<Integer> contentLength,
                         final int provisionChunks) {
        logger.debug("Starting to receive request body.");
        final StreamSourceChannel channel = exchange.getRequestChannel();
        if (channel == null) {
            throw UndertowMessages.MESSAGES.requestChannelAlreadyProvided();
        }

        /*
         * NOTE: We could make this use buffers from Undertow's / XNio's buffer pool,
         * instead of allocating our own. On G1, I think it makes more sense to
         * allocate the buffers without pooling, though. As they are fixed size and
         * < 1MB, allocation and pooling shouldn't differ that much performance-wise.
         * Undertow's buffer pool consists of native buffer in most cases, while these
         * buffers are used exclusively on heap after receiving the bytes from the
         * socket.
         */
        ChunkyByteBuffer.fill(channel, provisionChunks, contentLength.isPresent() ? provisionChunks : maximumChunks,
                              new ChunkyByteBuffer.CompletionHandler() {
            @Override
            public void overflow() {
                if (logger.isWarnEnabled()) {
                    logger.warn("Request body too large; rejecting request from {}", getPeerHost(exchange));
                }
                rejectLargeRequest(exchange);
            }
            @Override
            public void failed(final Throwable e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Error while reading request body received from " + getPeerHost(exchange), e);
                }
                Connectors.executeRootHandler(exchange -> exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR)
                                                                  .endExchange(),
                                              exchange);
            }
            @Override
            public void completed(final InputStream body, final int bodyLength) {
                // Sanity check that the body size matches the content-length header, if it
                // was supplied.
                if (contentLength.map(l -> l != bodyLength).orElse(false)) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Bad request from {}; content length ({}) didn't match actual body length ({}).",
                                    getPeerHost(exchange), contentLength.orElse(-1), bodyLength);
                    }
                    exchange.setStatusCode(StatusCodes.BAD_REQUEST)
                            .endExchange();
                    return;
                }
                // First bump the global default for the number of chunks that we pre-allocate.
                final int newNumChunks = calculateChunks(bodyLength);
                int currentNumChunks;
                while ((currentNumChunks = preallocateChunks.get()) < newNumChunks) {
                    if (preallocateChunks.compareAndSet(currentNumChunks, newNumChunks)) {
                        logger.info("Updated default body buffer size from {} to {}",
                                     currentNumChunks * ChunkyByteBuffer.CHUNK_SIZE,
                                     newNumChunks * ChunkyByteBuffer.CHUNK_SIZE);
                        break;
                    }
                }
                // Pass the body on to the upstream handler.
                callback.accept(body, bodyLength);
            }
        });
    }
}
