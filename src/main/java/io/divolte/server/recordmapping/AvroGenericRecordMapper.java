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

package io.divolte.server.recordmapping;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Mapper for converting JSON-based data into Avro datums.
 * All Avro schema types are supported, with the restriction
 * that union schemas are only supported between null and
 * one other schema.
 * <p>
 * This does not extend the normal Jackson plumbing to support
 * this due to the need for Avro schema information to be passed
 * around (and available) during deserialization.
 */
@ParametersAreNonnullByDefault
public class AvroGenericRecordMapper {
    private final ObjectReader reader;

    /**
     * Construct a mapper.
     * @param reader An underlying object mapper to use for mapping primitive types,
     *               and for querying which deserialization features are active.
     */
    public AvroGenericRecordMapper(final ObjectReader reader) {
        this.reader = Objects.requireNonNull(reader);
    }

    /**
     * Convert a JSON node tree into an Avro datum using the supplied schema.
     *
     * @param jsonNode the JSON node to convert.
     * @param targetSchema the schema of the Avro datum to produce.
     * @return an Avro datum that conforms to the supplied schema.
     * @throws IOException if an error occurs while producing the Avro datum.
     */
    @Nullable
    public Object read(final TreeNode jsonNode,
                       final Schema targetSchema) throws IOException {
        try (final JsonParser parser = jsonNode.traverse()) {
            // The parser always needs to be primed to position it over the first token.
            parser.nextToken();
            return read(parser, targetSchema);
        }
    }

    /**
     * Produce an Avro datum using the supplied JSON event stream.
     * The event stream must already be positioned on the first token
     * to read, and will be consumed only as far as necessary to
     * produce the datum that conforms to the supplied Avro schema.
     *
     * @param parser the JSON parser which will produce the event stream
     *               for the datum.
     * @param targetSchema the schema of the Avro datum to produce.
     * @return an Avro datum that conforms to the supplied schema.
     * @throws IOException if an error occurs while producing the Avro datum.
     */
    @Nullable
    public Object read(final JsonParser parser,
                       final Schema targetSchema) throws IOException {
        final Object result;
        switch (targetSchema.getType()) {
            case NULL:
                result = readNull(parser, targetSchema);
                break;
            case RECORD:
                result = readRecord(parser, targetSchema);
                break;
            case ENUM:
                result = readEnum(parser, targetSchema);
                break;
            case ARRAY:
                result = readArray(parser, targetSchema);
                break;
            case MAP:
                result = readMap(parser, targetSchema);
                break;
            case UNION:
                result = readUnion(parser, targetSchema);
                break;
            case FIXED:
                result = readFixed(parser, targetSchema);
                break;
            case STRING:
                result = reader.readValue(parser, String.class);
                break;
            case BYTES:
                result = reader.readValue(parser, ByteBuffer.class);
                break;
            case INT:
                result = reader.readValue(parser, Integer.class);
                break;
            case LONG:
                result = reader.readValue(parser, Long.class);
                break;
            case FLOAT:
                result = reader.readValue(parser, Float.class);
                break;
            case DOUBLE:
                result = reader.readValue(parser, Double.class);
                break;
            case BOOLEAN:
                result = reader.readValue(parser, Boolean.class);
                break;
            default:
                throw JsonMappingException.from(parser, "Unknown schema type: " + targetSchema);
        }
        return result;
    }

    @Nullable
    private <T> T readNull(final JsonParser parser,
                           final Schema targetSchema) throws IOException {
        Preconditions.checkArgument(targetSchema.getType() == Schema.Type.NULL);
        if (parser.getCurrentToken() != JsonToken.VALUE_NULL) {
            throw mappingException(parser, targetSchema);
        }
        return null;
    }

    private GenericRecord readRecord(final JsonParser parser,
                                     final Schema targetSchema) throws IOException {
        Preconditions.checkArgument(targetSchema.getType() == Schema.Type.RECORD);
        final GenericRecord result;
        // The parser can be placed in several positions on entry.
        switch (parser.getCurrentToken()) {
            case START_OBJECT:
                // This is fine. Advance to next token and re-enter.
                parser.nextToken();
                result = readRecord(parser, targetSchema);
                break;
            case END_OBJECT:
            case FIELD_NAME:
                result = new GenericData.Record(targetSchema);
                while (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
                    final String fieldName = parser.getCurrentName();
                    // Advance to the field value.
                    parser.nextToken();
                    // Process the field value according to the schema type.
                    final Schema.Field field = targetSchema.getField(fieldName);
                    if (null != field) {
                        final Object fieldValue = read(parser, field.schema());
                        result.put(field.pos(), fieldValue);
                    } else if (reader.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)) {
                        throw unrecognizedPropertyException(parser, result, fieldName);
                    } else {
                        // We're ignoring unknown properties; skip over this one.
                        parser.skipChildren();
                    }
                    // Advance to next token, in preparation of next field.
                    parser.nextToken();
                }
                break;
            default:
                throw mappingException(parser, targetSchema);
        }
        return result;
    }

    private String readEnum(final JsonParser parser,
                            final Schema targetSchema) throws IOException {
        Preconditions.checkArgument(targetSchema.getType() == Schema.Type.ENUM);
        String symbol = reader.readValue(parser, String.class);
        if (!targetSchema.hasEnumSymbol(symbol)) {
            if (reader.isEnabled(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)) {
                symbol = null;
            } else {
                throw unknownEnumValueException(parser, targetSchema, symbol);
            }
        }
        return symbol;
    }

    private List<?> readArray(final JsonParser parser,
                              final Schema targetSchema) throws IOException {
        Preconditions.checkArgument(targetSchema.getType() == Schema.Type.ARRAY);
        final List<?> result;
        final Schema elementType = targetSchema.getElementType();
        if (parser.isExpectedStartArrayToken()) {
            final List<Object> builder = new ArrayList<>();
            while (JsonToken.END_ARRAY != parser.nextToken()) {
                builder.add(read(parser, elementType));
            }
            result = builder;
        } else if (reader.isEnabled(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)) {
            result = Collections.singletonList(read(parser, elementType));
        } else {
            throw mappingException(parser, targetSchema);
        }
        return result;
    }

    private Map<String,?> readMap(final JsonParser parser,
                                  final Schema targetSchema) throws IOException {
        Preconditions.checkArgument(targetSchema.getType() == Schema.Type.MAP);
        final Map<String,?> result;
        // The parser can be placed in several positions on entry.
        switch (parser.getCurrentToken()) {
            case START_OBJECT:
                // This is fine. Advance to next token and re-enter.
                parser.nextToken();
                result = readMap(parser, targetSchema);
                break;
            case END_OBJECT:
                // Empty map. Nothing to do.
                result = Collections.emptyMap();
                break;
            case FIELD_NAME:
                final Map<String,Object> builder = new LinkedHashMap<>();
                do {
                    final String entryName = parser.getCurrentName();
                    // Advance to the value.
                    parser.nextToken();
                    final Object entryValue = read(parser, targetSchema.getValueType());
                    builder.put(entryName, entryValue);
                } while (parser.nextToken() == JsonToken.FIELD_NAME);
                result = builder;
                break;
            default:
                throw mappingException(parser, targetSchema);
        }
        return result;
    }

    private Object readUnion(final JsonParser parser,
                             final Schema targetSchema) throws IOException {
        Preconditions.checkArgument(targetSchema.getType() == Schema.Type.UNION);
        final List<Schema> possibleSchemas = targetSchema.getTypes();
        /*
         * We only allow unions of a specific type with null, and null must
         * be first. (It normally is, because that's the only way the field
         * can default to null.)
         *
         * The alternative here would be to replace the JsonParser with a TokenBuffer
         * and try to read as each possible schema type, until something succeeds.
         * This would be very expensive though, so for now it's not supported.
         */
        final Object result;
        final Iterator<Schema> possibleSchemesIterator = possibleSchemas.iterator();
        switch (possibleSchemas.size()) {
            case 2:
                final Schema nullSchema = possibleSchemesIterator.next();
                if (nullSchema.getType() != Schema.Type.NULL) {
                    throw unsupportedUnionException(parser, targetSchema);
                }
                // Check if we're on null.
                if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
                    result = null;
                    break;
                }
                // Intentional fall-through.
            case 1:
                final Schema resolvedSchema = possibleSchemesIterator.next();
                result = read(parser, resolvedSchema);
                break;
            default:
                // Not acceptable.
                throw unsupportedUnionException(parser, targetSchema);
        }
        return result;
    }

    private GenericFixed readFixed(final JsonParser parser,
                                   final Schema targetSchema) throws IOException {
        Preconditions.checkArgument(targetSchema.getType() == Schema.Type.FIXED);
        final byte[] bytes = reader.readValue(parser, byte[].class);
        return new GenericData.Fixed(targetSchema, bytes);
    }

    private static JsonMappingException mappingException(final JsonParser parser,
                                                         final Schema targetSchema) {
        return mappingException(parser, targetSchema, parser.getCurrentToken());
    }

    private static JsonMappingException mappingException(final JsonParser parser,
                                                         final Schema targetSchema,
                                                         final JsonToken token) {
        return JsonMappingException.from(parser, "Cannot read " + token + " as " + targetSchema);
    }

    private static UnrecognizedPropertyException unrecognizedPropertyException(final JsonParser parser,
                                                                               final GenericRecord record,
                                                                               final String fieldName) {
        final List<Object> fieldNames = record.getSchema().getFields()
                .stream()
                .map(Schema.Field::name)
                .collect(Collectors.toList());
        return UnrecognizedPropertyException.from(parser, record, fieldName, fieldNames);
    }

    private static InvalidFormatException unknownEnumValueException(final JsonParser parser,
                                                                    final Schema targetSchema,
                                                                    final String symbol) {
        return InvalidFormatException.from(parser,
                                           "Symbol " + symbol + " is not valid for enumeration " + targetSchema.getName(),
                                           symbol,
                                           null);
    }

    private static JsonMappingException unsupportedUnionException(final JsonParser parser,
                                                                  final Schema targetSchema) {
        return JsonMappingException.from(parser,
                                         "Unsupported union " + targetSchema.getName() + " encountered; unions are only supported with null as the first type.");
    }
}
