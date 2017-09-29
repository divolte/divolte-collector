/*
 * Copyright 2017 GoDataDriven B.V.
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

import io.divolte.server.AvroRecordBuffer;
import org.apache.kafka.common.serialization.Serializer;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.ByteBuffer;
import java.util.Map;

@ParametersAreNonnullByDefault
class ConfluentAvroRecordBufferSerializer implements Serializer<AvroRecordBuffer> {
    private static final int CONFLUENT_RECORD_MAGIC = 0;

    private final byte[] header;

    public ConfluentAvroRecordBufferSerializer(final int schemaId) {
        this.header = createConfluentRecordHeader(schemaId);
    }

    private static byte[] createConfluentRecordHeader(final int schemaId) {
        // Reference: https://docs.confluent.io/3.3.0/schema-registry/docs/serializer-formatter.html#wire-format
        // (The documentation doesn't specify the byte-order, but it's network byte order.)
        final byte[] header = new byte[5];
        header[0] = CONFLUENT_RECORD_MAGIC;
        header[1] = (byte) ((schemaId >> 24) & 0xff);
        header[2] = (byte) ((schemaId >> 16) & 0xff);
        header[3] = (byte) ((schemaId >> 8)  & 0xff);
        header[4] = (byte) ( schemaId        & 0xff);
        return header;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Nothing to do.
    }

    @Override
    public final byte[] serialize(final String topic, final AvroRecordBuffer data) {
        // Confluent format is the header (pre-calculated), and then the Avro record bytes.
        // (There's no way to do this without copying the array.)
        final ByteBuffer avroBuffer = data.getByteBuffer();
        final int recordBodySize = avroBuffer.remaining();
        final byte[] confluentRecordBytes = new byte[header.length + recordBodySize];
        System.arraycopy(header, 0, confluentRecordBytes, 0, header.length);
        avroBuffer.get(confluentRecordBytes, header.length, recordBodySize);
        return confluentRecordBytes;
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}
