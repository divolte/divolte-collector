/*
 * Copyright 2016 GoDataDriven B.V.
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

package io.divolte.server.kafka;

import org.apache.kafka.common.serialization.Serializer;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class ConfluentDivolteSerializer<T> implements Serializer<T> {
    private final byte[] header;

    ConfluentDivolteSerializer(Integer schemaId) {
        header = new byte[5];
        header[0] = 0;
        header[1] = (byte) ((schemaId >> 24) & 0xff);
        header[2] = (byte) ((schemaId >> 16) & 0xff);
        header[3] = (byte) ((schemaId >> 8) & 0xff);
        header[4] = (byte) (schemaId & 0xff);
    }

    public final byte[] serialize(final String topic, final T data) {
        // Extract the AVRO record as a byte array.
        // (There's no way to do this without copying the array.)
        ByteBuffer rawBytes = serializeData(data);
        return serializeWithConfluentPrefix(rawBytes);
    }

    protected abstract ByteBuffer serializeData(final T data);

    private byte[] serializeWithConfluentPrefix(ByteBuffer avroBuffer) {
        final int recordSize = avroBuffer.remaining();
        byte[] avroBytes = new byte[header.length + recordSize];
        System.arraycopy(header, 0, avroBytes, 0, header.length);
        avroBuffer.get(avroBytes, header.length, recordSize);
        return avroBytes;
    }

    @ParametersAreNonnullByDefault
    final static class ByteBufferOutputStream extends OutputStream {
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
