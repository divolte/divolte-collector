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

import avro.shaded.com.google.common.collect.ImmutableList;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.divolte.server.DivolteIdentifier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Map;

@ParametersAreNonnullByDefault
class ConfluentDivolteIdentifierSerializer implements Serializer<DivolteIdentifier> {

    private static final char DIVOLTE_IDENTIFIER_SCHEMA_VERSION = '0';
    static final Schema DIVOLTE_IDENTIFIER_SCHEMA = Schema.createRecord(ImmutableList.of(
        new Schema.Field("version", Schema.create(Schema.Type.STRING), "Divolte Identifier Version", (Object) null),
        new Schema.Field("timestamp", Schema.create(Schema.Type.LONG), "Timestamp", (Object) null),
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "Message identifier", (Object) null)
    ));

    private final KafkaAvroSerializer kas;

    public ConfluentDivolteIdentifierSerializer(int schemaId) {
        this.kas = new KafkaAvroSerializer(new SingleSchemaRegistryClient(schemaId));
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Nothing needed here.
    }

    @Override
    public byte[] serialize(String topic, DivolteIdentifier identifier) {
        assert identifier.version == DIVOLTE_IDENTIFIER_SCHEMA_VERSION;
        final GenericRecord record = new GenericData.Record(DIVOLTE_IDENTIFIER_SCHEMA);
        record.put("version", "" + identifier.version);
        record.put("timestamp", identifier.timestamp);
        record.put("id", identifier.getId());
        return kas.serialize(topic, record);
    }

    @Override
    public void close() {
        // Nothing needed here.
    }

}
