package io.divolte.server;

import com.google.common.primitives.Ints;
import io.undertow.UndertowMessages;
import io.undertow.server.Connectors;
import io.undertow.server.HttpServerExchange;
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
import java.util.function.Consumer;

@ParametersAreNonnullByDefault
public class AsyncRequestBodyReceiver {
    private static final Logger logger = LoggerFactory.getLogger(AsyncRequestBodyReceiver.class);

    private static final AtomicInteger NUM_CHUNKS = new AtomicInteger(1);

    private static final InputStream EMPTY_INPUT_STREAM = new InputStream() {
        @Override
        public int read() throws IOException {
            return -1;
        }
    };

    private final int numSlots;

    public AsyncRequestBodyReceiver(final int requestedMaxBufferSize) {
        /*
         * Convert requested buffer size into the number of slots.
         * We always need at least one slot, and the actual buffer
         * may be larger than requested because we always use a slot
         * completely.
         */
        final int requestedSlots = calculateChunks(requestedMaxBufferSize);
        numSlots = Math.max(requestedSlots, 1);
        logger.debug("Configuring to use {} buffer slots.", numSlots);
    }

    private static int calculateChunks(final int length) {
        // Should be faster than using Math.ceil().
        return (length - 1) / ChunkyByteBuffer.CHUNK_SIZE + 1;
    }

    public void receive(final Consumer<InputStream> callback, final HttpServerExchange exchange) {
        Objects.requireNonNull(callback);
        if (logger.isDebugEnabled()) {
            logger.debug("Number of buffers: {}", NUM_CHUNKS.get());
        }

        if (exchange.isRequestComplete()) {
            callback.accept(EMPTY_INPUT_STREAM);
            return;
        }

        final StreamSourceChannel channel = exchange.getRequestChannel();
        if (channel == null) {
            throw UndertowMessages.MESSAGES.requestChannelAlreadyProvided();
        }

        // Determine the number of buffer-slots to use, trusting a well-formed content-length
        // header if that's present.
        final int provision = Optional.ofNullable(exchange.getRequestHeaders().getFirst(Headers.CONTENT_LENGTH))
                                      .map(Ints::tryParse)
                                      .map(AsyncRequestBodyReceiver::calculateChunks)
                                      .orElseGet(NUM_CHUNKS::get);
        if (provision > numSlots) {
            exchange.setStatusCode(StatusCodes.REQUEST_ENTITY_TOO_LARGE).endExchange();
            return;
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
        ChunkyByteBuffer.fill(channel, provision, numSlots, new ChunkyByteBuffer.CompletionHandler() {
            @Override
            public void overflow() {
                exchange.setStatusCode(StatusCodes.REQUEST_ENTITY_TOO_LARGE).endExchange();
            }
            @Override
            public void failed(final Throwable e) {
                if (logger.isWarnEnabled()) {
                    final String host = Optional.ofNullable(exchange.getSourceAddress())
                            .map(InetSocketAddress::getHostString)
                            .orElse("<UNKNOWN HOST>");

                    logger.warn("Error while reading request body received from " + host, e);
                }
                Connectors.executeRootHandler(exchange -> exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR)
                                                                  .endExchange(),
                                              exchange);
            }
            @Override
            public void completed(final InputStream body, final int bodyLength) {
                // First bump the global default for the number of chunks that we pre-allocate.
                final int newNumChunks = calculateChunks(bodyLength);
                int currentNumChunks;
                while ((currentNumChunks = NUM_CHUNKS.get()) < newNumChunks) {
                    if (NUM_CHUNKS.compareAndSet(currentNumChunks, newNumChunks)) {
                        logger.info("Updated default body buffer size from {} to {}",
                                     currentNumChunks * ChunkyByteBuffer.CHUNK_SIZE,
                                     newNumChunks * ChunkyByteBuffer.CHUNK_SIZE);
                        break;
                    }
                }
                // Pass the body on to the upstream handler.
                callback.accept(body);
            }
        });
    }
}
