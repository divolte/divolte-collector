package io.divolte.server;

import io.undertow.server.HttpServerExchange;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;

/*
 * This class is called maker, because builder was already taken by Avro itself.
 */
final class GenericRecordMaker {
    private final Schema schema;
    private final Map<String, Pattern> regexes;

    public GenericRecordMaker(Schema schema, Config config) {
        final int version = config.getInt("divolte.tracking.schema_mapping.version");
        checkVersion(version);

        regexes = regexMapFromConfig(config);

        this.schema = schema;
    }

    private Map<String,Pattern> regexMapFromConfig(Config config) {
        return config.hasPath("divolte.tracking.schema_mapping.regexes") ?
        config.getConfig("divolte.tracking.schema_mapping.regexes").entrySet().stream().collect(
                Collectors.<Entry<String,ConfigValue>, String, Pattern>toMap(
                (e) -> e.getKey(),
                (e) -> {
                    if (e.getValue().valueType() != ConfigValueType.STRING) {
                        throw new SchemaMappingException(String.format("Regexes config elements must be of type STRING. Found %s of type %s.", e.getKey(), e.getValue().valueType()));
                    }
                    return Pattern.compile((String) e.getValue().unwrapped());
                })) : Collections.emptyMap();
    }

    private void checkVersion(final int version) {
        if (version != 1) {
            throw new SchemaMappingException(String.format("Unsupported schema mapping configuration version: %d", version));
        }
    }

    private interface FieldSetter {
        void setField(GenericRecordBuilder builder, HttpServerExchange exchange);
    }

    public GenericRecord makeRecordFromExchange(HttpServerExchange exchange) {
        return new GenericRecordBuilder(schema).build();
    }

    public class SchemaMappingException extends RuntimeException {
        private static final long serialVersionUID = 5856826064089770832L;

        public SchemaMappingException() {
            super();
        }

        public SchemaMappingException(String message, Throwable cause) {
            super(message, cause);
        }

        public SchemaMappingException(String message) {
            super(message);
        }

        public SchemaMappingException(Throwable cause) {
            super(cause);
        }
    }
}
