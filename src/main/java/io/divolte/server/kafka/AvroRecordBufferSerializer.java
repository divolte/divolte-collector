/*
 * Copyright 2015 GoDataDriven B.V.
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
import io.divolte.server.DivolteSchema;
import io.divolte.server.config.KafkaSinkMode;
import org.apache.kafka.common.serialization.Serializer;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

@ParametersAreNonnullByDefault
class AvroRecordBufferSerializer implements Serializer<AvroRecordBuffer> {

    private final KafkaSinkMode mode;
    private final Optional<Integer> schemaId;

    public AvroRecordBufferSerializer(KafkaSinkMode mode, DivolteSchema divolteSchema) {
        this.mode = mode;
        this.schemaId = divolteSchema.id;
        if (mode == KafkaSinkMode.CONFLUENT && !schemaId.isPresent()) {
            throw new IllegalStateException("No schema ID present for writing out Confluent compatible Kafka records");
        }
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Nothing to do.
    }

    @Override
    public byte[] serialize(final String topic, final AvroRecordBuffer data) {
        // Extract the AVRO record as a byte array.
        // (There's no way to do this without copying the array.)
        final ByteBuffer avroBuffer = data.getByteBuffer();
        final int recordSize = avroBuffer.remaining();
        final byte[] avroBytes;
        final int offset;
        switch (mode) {
            case CONFLUENT:
                int id = schemaId.get();
                avroBytes = new byte[5 + recordSize];
                avroBytes[0] = 0;
                avroBytes[1] = (byte) ((id >> 24) & 0xff);
                avroBytes[2] = (byte) ((id >> 16) & 0xff);
                avroBytes[3] = (byte) ((id >> 8) & 0xff);
                avroBytes[4] = (byte) (id & 0xff);
                offset = 5;
                break;

            default:
                avroBytes = new byte[recordSize];
                offset = 0;
        }
        avroBuffer.get(avroBytes, offset, recordSize);
        return avroBytes;
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}
