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

import io.divolte.record.DivolteIdentifierRecord;
import io.divolte.server.DivolteIdentifier;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

@ParametersAreNonnullByDefault
class ConfluentDivolteIdentifierSerializer extends ConfluentDivolteSerializer<DivolteIdentifier> {
    private static final int INITIAL_BUFFER_SIZE = 100;

    private static final char DIVOLTE_IDENTIFIER_SCHEMA_VERSION = '0';
    private static final SpecificDatumWriter<DivolteIdentifierRecord> DIVOLTE_IDENTIFIER_RECORD_WRITER =
        new SpecificDatumWriter<>(DivolteIdentifierRecord.class);

    ConfluentDivolteIdentifierSerializer(int schemaId) {
        super(schemaId);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Nothing needed here.
    }

    @Override
    protected ByteBuffer serializeData(final DivolteIdentifier identifier) {
        assert identifier.version == DIVOLTE_IDENTIFIER_SCHEMA_VERSION;

        final DivolteIdentifierRecord record = DivolteIdentifierRecord.newBuilder()
            .setVersion("" + identifier.version)
            .setTimestamp(identifier.timestamp)
            .setId(identifier.getId())
            .build();
        final ByteBuffer byteBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
        final ByteBufferOutputStream outputStream = new ByteBufferOutputStream(byteBuffer);
        final Encoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
        try {
            DIVOLTE_IDENTIFIER_RECORD_WRITER.write(record, encoder);
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to serialize divolte identifier", ioe);
        }

        // Prepare buffer for reading, and store it (read-only).
        byteBuffer.flip();
        return byteBuffer.asReadOnlyBuffer();
    }

    @Override
    public void close() {
        // Nothing needed here.
    }

}
