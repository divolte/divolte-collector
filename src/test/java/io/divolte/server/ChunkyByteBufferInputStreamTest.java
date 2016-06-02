package io.divolte.server;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;
import io.divolte.server.ChunkyByteBuffer.ChunkyByteBufferInputStream;
import org.junit.Test;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@ParametersAreNonnullByDefault
public class ChunkyByteBufferInputStreamTest {
    @Test
    public void shouldEofOnNoBuffers() throws IOException {
        try (final InputStream stream = new ChunkyByteBufferInputStream()) {
            assertStreamEof(stream);
        }
    }

    @Test
    public void shouldEofOnSingleEmptyBuffers() throws IOException {
        try (final InputStream stream = new ChunkyByteBufferInputStream(ByteBuffer.allocate(10))) {
            assertStreamEof(stream);
        }
    }

    @Test
    public void shouldEofOnAllEmptyBuffers() throws IOException {
        try (final InputStream stream = new ChunkyByteBufferInputStream(ByteBuffer.allocate(10), ByteBuffer.allocate(20))) {
            assertStreamEof(stream);
        }
    }

    @Test
    public void shouldEofOnAllNullBuffers() throws IOException {
        try (final InputStream stream = new ChunkyByteBufferInputStream(null, null, null)) {
            assertStreamEof(stream);
        }
    }

    private void assertStreamEof(final InputStream stream) throws IOException {
        assertEquals(0, stream.available());
        assertEquals(-1, stream.read());
        assertEquals(-1, stream.read(new byte[10]));
        assertEquals(-1, stream.read(new byte[10], 0, 10));
    }

    @Test
    public void shouldReadSingleBuffer() throws IOException {
        final byte[] sentinelBytes = "someTextToRead".getBytes(StandardCharsets.UTF_8);
        final ByteBuffer buffer = ByteBuffer.allocate(sentinelBytes.length + 10).put(sentinelBytes);

        try (final InputStream stream = new ChunkyByteBufferInputStream(buffer)) {
            final byte[] readBytes = ByteStreams.toByteArray(stream);
            assertArrayEquals(sentinelBytes, readBytes);
        }
    }

    @Test
    public void shouldReadMultipleBuffer() throws IOException {
        final byte[] sentinelBytes1 = "someTextToRead".getBytes(StandardCharsets.UTF_8);
        final byte[] sentinelBytes2 = "moreTextToRead".getBytes(StandardCharsets.UTF_8);

        final ByteBuffer buffer1 = ByteBuffer.allocate(sentinelBytes1.length + 10).put(sentinelBytes1);
        final ByteBuffer buffer2 = ByteBuffer.allocate(sentinelBytes1.length + 10).put(sentinelBytes2);

        try (final InputStream stream = new ChunkyByteBufferInputStream(buffer1, buffer2)) {
            final byte[] readBytes = ByteStreams.toByteArray(stream);
            assertArrayEquals(Bytes.concat(sentinelBytes1, sentinelBytes2), readBytes);
        }
    }

    @Test
    public void shouldSkipNullBuffers() throws IOException {
        final byte[] sentinelBytes1 = "someTextToRead".getBytes(StandardCharsets.UTF_8);
        final byte[] sentinelBytes2 = "moreTextToRead".getBytes(StandardCharsets.UTF_8);
        final ByteBuffer buffer1 = ByteBuffer.allocate(sentinelBytes1.length + 10).put(sentinelBytes1);
        final ByteBuffer buffer2 = ByteBuffer.allocate(sentinelBytes1.length + 10).put(sentinelBytes2);

        try (final InputStream stream = new ChunkyByteBufferInputStream(buffer1, null, buffer2)) {
            final byte[] readBytes = ByteStreams.toByteArray(stream);
            assertArrayEquals(Bytes.concat(sentinelBytes1, sentinelBytes2), readBytes);
        }
    }

    @Test
    public void shouldDribbleMultipleBuffers() throws IOException {
        final byte[] sentinelBytes1 = "someTextToRead".getBytes(StandardCharsets.UTF_8);
        final byte[] sentinelBytes4 = "moreTextToRead".getBytes(StandardCharsets.UTF_8);
        final ByteBuffer buffer1 = ByteBuffer.allocate(sentinelBytes1.length + 10).put(sentinelBytes1);
        final ByteBuffer buffer2 = ByteBuffer.allocate(10);
        final ByteBuffer buffer4 = ByteBuffer.allocate(sentinelBytes1.length + 10).put(sentinelBytes4);

        final byte[] outputArray = new byte[sentinelBytes1.length + sentinelBytes4.length];
        try (final InputStream stream = new ChunkyByteBufferInputStream(buffer1, buffer2, null, buffer4)) {
            int cursor = 0, b;
            while (-1 != (b = stream.read())) {
                outputArray[cursor++] = (byte)b;
            }
            assertEquals(outputArray.length, cursor);
        }
        assertArrayEquals(Bytes.concat(sentinelBytes1, sentinelBytes4), outputArray);
    }
}
