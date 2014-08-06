package io.divolte.server;

import io.undertow.server.HttpServerExchange;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.typesafe.config.Config;

/*
 * This class is called maker, because builder was already taken by Avro itself.
 */
final class GenericRecordMaker {
    private final Schema schema;

    public GenericRecordMaker(Schema schema, Config config) {
        this.schema = schema;
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
