package io.divolte.server.kafka;

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
