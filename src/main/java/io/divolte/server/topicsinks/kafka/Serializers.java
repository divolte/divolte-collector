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

package io.divolte.server.topicsinks.kafka;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteIdentifier;
import io.divolte.server.DivolteSchema;
import org.apache.kafka.common.serialization.Serializer;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public final class Serializers {
    private Serializers() {
        // Prevent external instantiation.
    }

    static public Serializer<DivolteIdentifier> createKeySerializer() {
        return new DivolteIdentifierSerializer();
    }

    static public Serializer<AvroRecordBuffer> createNakedAvroSerializer(@SuppressWarnings("unused") final DivolteSchema schema) {
        return new AvroRecordBufferSerializer();
    }

    static public Serializer<AvroRecordBuffer> createConfluentAvroSerializer(final DivolteSchema schema) {
        return schema.confluentId
            .map(ConfluentAvroRecordBufferSerializer::new)
            .orElseThrow(() -> new IllegalArgumentException("Cannot create Confluent-compatible serializer without registry id"));
    }
}
