package io.divolte.server;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.ChannelListener;
import org.xnio.channels.StreamSourceChannel;

import com.google.common.primitives.Longs;

import io.undertow.UndertowMessages;
import io.undertow.server.Connectors;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;

@ParametersAreNonnullByDefault
public class AsyncRequestBodyReceiver {
    private static final Logger logger = LoggerFactory.getLogger(AsyncRequestBodyReceiver.class);

    private static final int BYTE_BUFFER_SIZE = 1024;
    private static final AtomicInteger NUM_BUFFERS = new AtomicInteger(1);

    private final static InputStream EMPTY_INPUT_STREAM = new InputStream() {
        @Override
        public int read() throws IOException {
            return -1;
        }
    };

    private final int maxBufferSize;
    private final int numSlots;

    public AsyncRequestBodyReceiver(final int requestedMaxBufferSize) {
        /*
         * Require a minimal max buffer size of BYTE_BUFFER_SIZE and
         * round up to the next multiple of BYTE_BUFFER_SIZE
         */
        this.maxBufferSize = ((Math.max(requestedMaxBufferSize, BYTE_BUFFER_SIZE) - 1) / BYTE_BUFFER_SIZE + 1) * BYTE_BUFFER_SIZE;
        numSlots = maxBufferSize / BYTE_BUFFER_SIZE;
    }

    public void receive(
            final Consumer<InputStream> callback,
            final HttpServerExchange exchange) {
        Objects.requireNonNull(callback);

        logger.debug("nb" + NUM_BUFFERS.get());

        if (exchange.isRequestComplete()) {
            callback.accept(EMPTY_INPUT_STREAM);
            return;
        }

        final StreamSourceChannel channel = exchange.getRequestChannel();
        if (channel == null) {
            throw UndertowMessages.MESSAGES.requestChannelAlreadyProvided();
        }

        final long provision = Optional.ofNullable(exchange.getRequestHeaders().getFirst(Headers.CONTENT_LENGTH))
                                       .map(Longs::tryParse) // Note that we're lenient about malformed numbers here and treat them as absent
                                       .map(size -> (size - 1) / BYTE_BUFFER_SIZE + 1)
                                       .orElse(Long.valueOf(NUM_BUFFERS.get()));

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
        final ByteBuffer[] buffers = new ByteBuffer[numSlots];
        IntStream.range(0, (int) provision).forEach(idx -> buffers[idx] = ByteBuffer.allocate(BYTE_BUFFER_SIZE));

        int currentBuffer = 0;
        int readResult;
        do {
            try {
                readResult = channel.read(buffers[currentBuffer]);
            } catch (final IOException e) {
                exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR).endExchange();
                logger.warn("Error while reading request body received from {}.", Optional.ofNullable(exchange.getSourceAddress()).map(InetSocketAddress::getHostString).orElse("<UNKNOWN HOST>"), e);
                return;
            }

            switch(readResult) {
            case -1:
                callback.accept(new ByteBufferArrayInputStream(buffers, currentBuffer));

                for (int currentNumBuffers = NUM_BUFFERS.get(); currentNumBuffers < currentBuffer + 1; currentNumBuffers = NUM_BUFFERS.get()) {
                    NUM_BUFFERS.compareAndSet(currentNumBuffers, currentBuffer + 1);
                }
                break;
            case 0:
                final int finalCurrentBuffer = currentBuffer; // Trick to pass on a variable to the inner type
                channel.getReadSetter().set(new ChannelListener<StreamSourceChannel>() {
                    int currentBuffer = finalCurrentBuffer;
                    @Override
                    public void handleEvent(final StreamSourceChannel channel) {
                        int readResult;
                        do {
                            try {
                                readResult = channel.read(buffers[currentBuffer]);
                            } catch (final IOException e) {
                                Connectors.executeRootHandler(exchange -> {
                                    exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR).endExchange();
                                    logger.warn("Error while reading request body received from {}.", Optional.ofNullable(exchange.getSourceAddress()).map(InetSocketAddress::getHostString).orElse("<UNKNOWN HOST>"), e);
                                }, exchange);
                                return;
                            }
                            switch(readResult) {
                            case -1:
                                Connectors.executeRootHandler(exchange -> {
                                    callback.accept(new ByteBufferArrayInputStream(buffers, currentBuffer));
                                }, exchange);

                                for (int currentNumBuffers = NUM_BUFFERS.get(); currentNumBuffers < currentBuffer + 1; currentNumBuffers = NUM_BUFFERS.get()) {
                                    NUM_BUFFERS.compareAndSet(currentNumBuffers, currentBuffer + 1);
                                }

                                break;
                            case 0:
                                // No bytes available right now, keep listening.
                                break;
                            default:
                                if (!buffers[currentBuffer].hasRemaining() &&
                                        ++currentBuffer < numSlots &&
                                        buffers[currentBuffer] == null) {
                                    buffers[currentBuffer] = ByteBuffer.allocate(BYTE_BUFFER_SIZE);
                                }
                                break;
                            }
                        } while (readResult > 0 && (buffers[currentBuffer].hasRemaining() || currentBuffer < numSlots - 1));
                    }
                });
                channel.resumeReads();
                break;
            default:
                if (!buffers[currentBuffer].hasRemaining() &&
                        ++currentBuffer < numSlots &&
                        buffers[currentBuffer] == null) {
                    buffers[currentBuffer] = ByteBuffer.allocate(BYTE_BUFFER_SIZE);
                }
                break;
            }
        } while (
                readResult > 0 &&                             // There's possiby more bytes to read AND
                (
                        buffers[currentBuffer].hasRemaining() // There's bytes remaining in the current buffer OR
                        || currentBuffer < numSlots - 1       // it's possible to allocate another one
                ));
    }

    private static final class ByteBufferArrayInputStream extends InputStream {
        private final ByteBuffer[] buffers;
        private final int maxBufferIndex;

        private int currentBuffer;

        private ByteBufferArrayInputStream(final ByteBuffer[] buffers, final int maxBufferIndex) {
            Stream.of(buffers).filter(b -> b != null).forEach(ByteBuffer::flip);
            this.buffers = buffers;
            this.maxBufferIndex = maxBufferIndex;
            currentBuffer = 0;
        }

        @Override
        public int read() throws IOException {
            if (buffers[currentBuffer].hasRemaining()) {
                return buffers[currentBuffer].get();
            } else {
                return currentBuffer == maxBufferIndex ?
                        -1 :
                        buffers[++currentBuffer].hasRemaining() ? buffers[currentBuffer].get() : -1;
            }
        }
    }
}
