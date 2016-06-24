package io.divolte.server;

import com.google.common.base.Preconditions;
import org.xnio.channels.StreamSourceChannel;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

@ParametersAreNonnullByDefault
public class ChunkyByteBuffer {
    public static final int CHUNK_SIZE = 4096;

    @ParametersAreNonnullByDefault
    public interface CompletionHandler {
        void completed(InputStream stream, int totalLength);
        void overflow();
        void failed(Throwable e);
    }

    private final CompletionHandler completionHandler;
    // The chunks into which we will be buffering data. Allocated on demand as they fill up.
    private final ByteBuffer[] chunks;
    private int currentChunkIndex;

    public static void fill(final StreamSourceChannel channel,
                            final int initialChunkCount,
                            final int maxChunkCount,
                            final CompletionHandler completionHandler) {
        final ChunkyByteBuffer buffer = new ChunkyByteBuffer(initialChunkCount, maxChunkCount, completionHandler);
        buffer.fillBuffers(channel, c -> {
            c.getReadSetter().set(d -> buffer.fillBuffers(d, null));
            c.resumeReads();
        });
    }

    private ChunkyByteBuffer(final int initialChunkCount,
                             final int maxChunkCount,
                             final CompletionHandler completionHandler) {
        Preconditions.checkArgument(0 < initialChunkCount && initialChunkCount <= maxChunkCount,
                                    "Initial chunk count (%s) must be greater than 0 and less than or equal to the maximum chunk count (%s)", initialChunkCount, maxChunkCount);
        this.completionHandler = Objects.requireNonNull(completionHandler);
        final int headChunkSize = initialChunkCount * CHUNK_SIZE;
        final int tailChunkCount = maxChunkCount - initialChunkCount;
        chunks = new ByteBuffer[1 + tailChunkCount];
        chunks[0] = ByteBuffer.allocate(headChunkSize);
    }

    private int getBufferUsed() {
        // The buffer size is calculated as the sum of:
        //  - Size of the first buffer, which is special.
        //  - Sizes of the 'middle' buffers, which are full.
        //  - Used portion of the last buffer.
        // (There are some corner cases when there's only a single buffer, or no middle ones.)
        int bufferSize = 0;
        switch (currentChunkIndex) {
            default:
                bufferSize += (currentChunkIndex - 1) * CHUNK_SIZE;
                // Deliberate fall-through.
            case 1:
                bufferSize += chunks[0].limit();
                // Deliberate fall-through.
            case 0:
                bufferSize += chunks[currentChunkIndex].position();
        }
        return bufferSize;
    }

    private void fillBuffers(final StreamSourceChannel channel,
                             @Nullable
                             final Consumer<StreamSourceChannel> willBlockHandler) {
        // Note: a few early-returns are sprinkled around.
        for (;;) {
            final ByteBuffer currentChunk = chunks[currentChunkIndex];
            final int readResult;
            try {
                readResult = channel.read(currentChunk);
            } catch (final IOException e) {
                onFailure(channel, e);
                return;
            }
            switch (readResult) {
                case -1:
                    // End-of-file.
                    endOfFile(channel);
                    return;
                case 0:
                    // Nothing pending right now.
                    if (null != willBlockHandler) {
                        willBlockHandler.accept(channel);
                    }
                    return;
                default:
                    // Some data was read:
                    //  - Might have filled current chunk; move on to next, if possible.
                    //  - Might be no more data; will check on next loop.
                    if (!currentChunk.hasRemaining()) {
                        if (currentChunkIndex + 1 < chunks.length) {
                            // We allocate chunks on demand, as we advance.
                            chunks[++currentChunkIndex] = ByteBuffer.allocate(CHUNK_SIZE);
                        } else {
                            // Buffers are full.
                            channel.getReadSetter().set(this::waitForEndOfStream);
                            channel.resumeReads();
                            return;
                        }
                    }
            }
        }
    }

    private void endOfFile(final StreamSourceChannel channel) {
        channel.getReadSetter().set(null);
        // Calculate the size before the buffers are flipped for reading.
        final int bufferSize = getBufferUsed();
        completionHandler.completed(new ChunkyByteBufferInputStream(chunks), bufferSize);
    }

    private void onFailure(final StreamSourceChannel channel, final Throwable t) {
        channel.getReadSetter().set(null);
        completionHandler.failed(t);
    }

    private void waitForEndOfStream(final StreamSourceChannel channel) {
        // So we have to supply a buffer to detect EOF.
        // This is a corner case, and some alternatives are inappropriate:
        //  - A singleton 1-byte buffer will lead to contention if multiple clients are in this state.
        //  - A zero-byte buffer can't be used because the semantics for reading from it aren't properly
        //    defined.
        final ByteBuffer tinyBuffer = ByteBuffer.allocate(1);
        try {
            final int numRead = channel.read(tinyBuffer);
            switch (numRead) {
                case -1:
                    // End-of-stream. Phew.
                    endOfFile(channel);
                    break;
                case 0:
                    // Still not sure. Wait.
                    break;
                default:
                    // Overflow. Doh.
                    channel.getReadSetter().set(null);
                    completionHandler.overflow();
            }
        } catch (final IOException e) {
            onFailure(channel, e);
        }
    }

    @ParametersAreNonnullByDefault
    static final class ChunkyByteBufferInputStream extends InputStream {
        // The buffers wrapped by this stream.
        // (These will be released as we no longer need them.)
        private final ByteBuffer[] buffers;
        // The index of the current buffer, or equal to the number of buffers if exhausted.
        private int currentBufferIndex;

        public ChunkyByteBufferInputStream(final ByteBuffer... buffers) {
            this.buffers = Stream.of(buffers)
                                 .filter(Objects::nonNull)
                                 .map(Buffer::flip)
                                 .toArray(ByteBuffer[]::new);
        }

        @Override
        public int available() throws IOException {
            while (currentBufferIndex < buffers.length) {
                final ByteBuffer currentBuffer = buffers[currentBufferIndex];
                final int remaining = currentBuffer.remaining();
                if (0 < remaining) {
                    return remaining;
                }
                buffers[currentBufferIndex++] = null;
            }
            return 0;
        }

        @Override
        public int read() throws IOException {
            while (currentBufferIndex < buffers.length) {
                final ByteBuffer currentBuffer = buffers[currentBufferIndex];
                if (currentBuffer.hasRemaining()) {
                    return currentBuffer.get();
                }
                buffers[currentBufferIndex++] = null;
            }
            return -1;
        }

        @Override
        public int read(final byte[] destination, final int offset, final int length) throws IOException {
            while (currentBufferIndex < buffers.length) {
                final ByteBuffer currentBuffer = buffers[currentBufferIndex];
                final int remaining = currentBuffer.remaining();
                if (0 < remaining) {
                    final int count = Math.min(remaining, length);
                    currentBuffer.get(destination, offset, count);
                    return count;
                }
                buffers[currentBufferIndex++] = null;
            }
            return -1;
        }
    }
}
