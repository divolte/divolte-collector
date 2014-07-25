package io.divolte.server;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

final class AvroRecordBuffer<T extends SpecificRecord> {
    private final static int INITIAL_BUFFER_SIZE = 100;
    private static final AtomicInteger bufferSize = new AtomicInteger(INITIAL_BUFFER_SIZE);

    private final byte[] buffer;
    private final int size;

    private AvroRecordBuffer(T record) throws IOException {
        this.buffer = new byte[bufferSize.get()];

        /*
         * We avoid ByteArryOutpuStream as it is fully synchronized and performs
         * a lot of copying. Instead, we create a byte array and point a
         * ByteBuffer to it and create a custom OutputStream implementation that
         * writes directly to the ByteBuffer. If we under-allocate, we recreate
         * the entire object using a larger byte array. All subsequent instances
         * will also allocate the larger size array from that point onward.
         */
        final DatumWriter<T> writer = new SpecificDatumWriter<>(record.getSchema());
        final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        final Encoder encoder = EncoderFactory.get().directBinaryEncoder(new ByteBufferOutputStream(byteBuffer), null);

        writer.write(record, encoder);
        size = byteBuffer.position();
    }

    public static <T extends SpecificRecord> AvroRecordBuffer<T> fromRecord(T record) {
        while (true) {
            try {
                return new AvroRecordBuffer<T>(record);
            } catch (BufferOverflowException boe) {
                // Increase the buffer size by about 10%
                // Because we only ever increase the buffer size, we discard the
                // scenario
                // where this thread fails to set the new size, as we can assume
                // another
                // thread increased it.
                int currentSize = bufferSize.get();
                bufferSize.compareAndSet(currentSize, (int) (currentSize * 1.1));
            } catch (IOException ioe) {
                throw new RuntimeException("Serialization error.", ioe);
            }
        }
    }

    public byte[] buffer() {
        return buffer;
    }

    public int size() {
        return size;
    }

    private final class ByteBufferOutputStream extends OutputStream {
        private final ByteBuffer underlying;

        public ByteBufferOutputStream(ByteBuffer underlying) {
            super();
            this.underlying = underlying;
        }

        @Override
        public void write(int b) throws IOException {
            underlying.put((byte) b);
        }
    }
}
