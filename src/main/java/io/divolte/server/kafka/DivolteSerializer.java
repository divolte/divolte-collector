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

import io.divolte.server.config.KafkaSinkMode;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Optional;

public abstract class DivolteSerializer<T> implements Serializer<T> {
    private final KafkaSinkMode mode;
    private final Optional<Integer> schemaId;

    DivolteSerializer(KafkaSinkMode mode, Optional<Integer> schemaId) {
        this.mode = mode;
        this.schemaId = schemaId;
        if (mode == KafkaSinkMode.CONFLUENT && !schemaId.isPresent()) {
            throw new IllegalStateException("No schema ID present for writing out Confluent compatible Kafka records");
        }
    }

    public final byte[] serialize(final String topic, final T data) {
        // Extract the AVRO record as a byte array.
        // (There's no way to do this without copying the array.)
        ByteBuffer rawBytes = serializeData(data, mode);
        switch (mode) {
            case CONFLUENT:
                return serializeWithConfluentPrefix(rawBytes);

            default:
                int recordSize = rawBytes.remaining();
                byte[] result = new byte[recordSize];
                rawBytes.get(result, 0, recordSize);
                return result;
        }
    }

    protected abstract ByteBuffer serializeData(final T data, KafkaSinkMode mode);

    private byte[] serializeWithConfluentPrefix(ByteBuffer avroBuffer) {
        final int recordSize = avroBuffer.remaining();
        int id = schemaId.get();
        byte[] avroBytes = new byte[5 + recordSize];
        avroBytes[0] = 0;
        avroBytes[1] = (byte) ((id >> 24) & 0xff);
        avroBytes[2] = (byte) ((id >> 16) & 0xff);
        avroBytes[3] = (byte) ((id >> 8) & 0xff);
        avroBytes[4] = (byte) (id & 0xff);
        avroBuffer.get(avroBytes, 5, recordSize);
        return avroBytes;
    }
}
