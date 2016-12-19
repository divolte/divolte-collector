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

import io.divolte.server.DivolteIdentifier;
import io.divolte.server.config.KafkaSinkMode;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

public class DivolteIdentifierSerializerTest {

    @Test
    public void serializerShouldPrependIdWithVersion() {
        DivolteIdentifierSerializer serializer = new DivolteIdentifierSerializer(KafkaSinkMode.NAKED, Optional.empty());
        DivolteIdentifier cv = DivolteIdentifier.generate(42);
        byte[] asBytes = serializer.serialize("topic", cv);
        assertEquals('0', asBytes[0]);
    }

    @Test
    public void serializerShouldWriteConfluentCompatible() {
        final int schemaId = 123456789;
        DivolteIdentifierSerializer serializer = new DivolteIdentifierSerializer(KafkaSinkMode.CONFLUENT, Optional.of(schemaId));
        DivolteIdentifier cv = DivolteIdentifier.generate(42);
        byte[] asBytes = serializer.serialize("topic", cv);
        assertEquals(0, asBytes[0]);
        int readId = ((int) asBytes[1] & 0xff) << 24 | ((int) asBytes[2] & 0xff) << 16 | ((int) asBytes[3] & 0xff) << 8 | ((int) asBytes[4] & 0xff);
        assertEquals(schemaId, readId);
    }
}
