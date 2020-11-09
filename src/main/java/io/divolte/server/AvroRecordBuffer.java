/*
 * Copyright 2019 GoDataDriven B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.divolte.server;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.base.MoreObjects;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

@ParametersAreNonnullByDefault
public final class AvroRecordBuffer {
    private static final int INITIAL_BUFFER_SIZE = 100;
    private static final AtomicInteger BUFFER_SIZE = new AtomicInteger(INITIAL_BUFFER_SIZE);
    static final GenericData AVRO_GENERIC_DATA;

    static {
        AVRO_GENERIC_DATA = new GenericData();
        AVRO_GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        AVRO_GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    }

    private final DivolteIdentifier partyId;
    private final DivolteIdentifier sessionId;
    private final String eventId;
    private final Instant timestamp;
    private final ByteBuffer byteBuffer;

    private AvroRecordBuffer(final DivolteIdentifier partyId,
                             final DivolteIdentifier sessionId,
                             final String eventId,
                             final Instant timestamp,
                             final GenericRecord record) throws IOException {
        this.partyId = Objects.requireNonNull(partyId);
        this.sessionId = Objects.requireNonNull(sessionId);
        this.eventId = Objects.requireNonNull(eventId);
        this.timestamp = Objects.requireNonNull(timestamp);

        /*
         * We avoid ByteArrayOutputStream as it is fully synchronized and performs
         * a lot of copying. Instead, we create a byte array and point a
         * ByteBuffer to it and create a custom OutputStream implementation that
         * writes directly to the ByteBuffer. If we under-allocate, we recreate
         * the entire object using a larger byte array. All subsequent instances
         * will also allocate the larger size array from that point onward.
         */
        final ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE.get());
        final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema(), AVRO_GENERIC_DATA);
        final Encoder encoder = EncoderFactory.get().directBinaryEncoder(new ByteBufferOutputStream(byteBuffer), null);

        writer.write(record, encoder);

        // Prepare buffer for reading, and store it (read-only).
        byteBuffer.flip();
        this.byteBuffer = byteBuffer.asReadOnlyBuffer();
    }

    public DivolteIdentifier getPartyId() {
        return partyId;
    }

    public DivolteIdentifier getSessionId() {
        return sessionId;
    }

    public String getEventId() {
        return eventId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public static AvroRecordBuffer fromRecord(final DivolteIdentifier partyId,
                                              final DivolteIdentifier sessionId,
                                              final String eventId,
                                              final Instant timestamp,
                                              final GenericRecord record) {
        for (;;) {
            try {
                return new AvroRecordBuffer(partyId, sessionId, eventId, timestamp, record);
            } catch (final BufferOverflowException boe) {
                // Increase the buffer size by about 10%
                // Because we only ever increase the buffer size, we discard the
                // scenario where this thread fails to set the new size,
                // as we can assume another thread increased it.
                int currentSize = BUFFER_SIZE.get();
                BUFFER_SIZE.compareAndSet(currentSize, (int) (currentSize * 1.1));
            } catch (final IOException ioe) {
                throw new UncheckedIOException("Serialization error.", ioe);
            }
        }
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer.slice();
    }

    /**
     * Convenience getter for determining the size without materializing a slice of the buffer.
     * @return The internal buffer's size.
     */
    public int size() {
        return byteBuffer.limit();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("partyId", getPartyId())
                .add("sessionId", getSessionId())
                .add("size", size())
                .toString();
    }

    @ParametersAreNonnullByDefault
    private static final class ByteBufferOutputStream extends OutputStream {
        private final ByteBuffer underlying;

        public ByteBufferOutputStream(final ByteBuffer underlying) {
            this.underlying = Objects.requireNonNull(underlying);
        }

        @Override
        public void write(final int b) throws IOException {
            underlying.put((byte) b);
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            underlying.put(b, off, len);
        }
    }
}
