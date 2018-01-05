/*
 * Copyright 2018 GoDataDriven B.V.
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

package io.divolte.server.topicsinks.kafka;

import io.divolte.record.DefaultEventRecord;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteIdentifier;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfluentAvroRecordBufferSerializerTest {

    @Test
    public void serializedRecordsAreConfluentCompatible() {
        final int schemaId = 0x1DEFACED;
        // Generate a record.
        final byte[] serializedRecord;
        try (final ConfluentAvroRecordBufferSerializer serializer = new ConfluentAvroRecordBufferSerializer(schemaId)) {
            serializer.configure(Collections.emptyMap(), false);
            serializedRecord = serializer.serialize("atopical", generateAvroRecord());
        }
        // Check the header.
        assertEquals((byte)0x00, serializedRecord[0]);
        assertEquals((byte)0x1d, serializedRecord[1]);
        assertEquals((byte)0xef, serializedRecord[2]);
        assertEquals((byte)0xac, serializedRecord[3]);
        assertEquals((byte)0xed, serializedRecord[4]);
        // And that there's a payload.
        assertTrue(5 < serializedRecord.length);
    }

    private static AvroRecordBuffer generateAvroRecord() {
        final GenericRecord record = new GenericRecordBuilder(DefaultEventRecord.getClassSchema())
            .set("detectedDuplicate", false)
            .set("detectedCorruption", false)
            .set("firstInSession", false)
            .set("timestamp", 0L)
            .set("clientTimestamp", 0L)
            .set("remoteHost", "localhost")
            .build();
        return AvroRecordBuffer.fromRecord(DivolteIdentifier.generate(0L),
                                           DivolteIdentifier.generate(1L),
                                           record);
    }
}
