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

package io.divolte.server.recordmapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.reflect.TypeToken;
import io.divolte.server.DivolteEvent;
import io.divolte.server.recordmapping.DslRecordMapping.PrimitiveValueProducer;
import io.divolte.server.recordmapping.DslRecordMapping.ValueProducer;
import org.apache.avro.Schema;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@ParametersAreNonnullByDefault
public final class Digester {
    private final String digestIdentifier;
    private final Supplier<MessageDigest> digestFactory;
    private List<ValueProducer<ByteBuffer>> digestPieces = new ArrayList<>();

    private Digester(final String identifier, final String algorithm) {
        digestIdentifier = Objects.requireNonNull(identifier);
        // Check the algorithm is valid.
        try {
            MessageDigest.getInstance(algorithm);
        } catch (final NoSuchAlgorithmException e) {
            final SchemaMappingException exception = new SchemaMappingException("Algorithm not supported for digester: %s", identifier);
            exception.initCause(e);
            throw exception;
        }
        digestFactory = () -> {
            try {
                return MessageDigest.getInstance(algorithm);
            } catch (final NoSuchAlgorithmException e) {
                throw new IllegalStateException("Cannot instantiate digester for algorithm: " + algorithm, e);
            }
        };
    }

    public ValueProducer<ByteBuffer> result() {
        final String identifier =
            digestPieces.stream()
                        .map(s -> String.format(".add(%s)", s))
                        .collect(Collectors.joining("", digestIdentifier, ".result()"));
        return new BytesValueProducer(identifier, this::calculateDigest);
    }

    private Optional<ByteBuffer> calculateDigest(final DivolteEvent e, final Map<String,Optional<?>> context) {
        final MessageDigest messageDigest = digestFactory.get();
        digestPieces.stream()
                    .map(x -> x.produce(e, context))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEachOrdered(messageDigest::update);
        return Optional.of(ByteBuffer.wrap(messageDigest.digest()));
    }

    public Digester add(final ValueProducer<?> piece) {
        final TypeToken<?> producerType = piece.producerType;
        if (producerType.isSubtypeOf(String.class)) {
            @SuppressWarnings("unchecked")
            final ValueProducer<String> stringPiece = (ValueProducer<String>) piece;
            return addStringValueProducer(stringPiece);
        } else if (producerType.isSubtypeOf(JsonNode.class)) {
            @SuppressWarnings("unchecked")
            final ValueProducer<JsonNode> jsonPiece = (ValueProducer<JsonNode>) piece;
            return addJsonValueProducer(jsonPiece);
        }
        throw new SchemaMappingException("Cannot digest value of type: %s", producerType);
    }

    private Digester add(final BytesValueProducer piece) {
        digestPieces.add(piece);
        return this;
    }

    private Digester addStringValueProducer(final ValueProducer<String> piece) {
        return add(new BytesValueProducer(piece.identifier,
                   (e, c) -> piece.produce(e, c).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))));
    }

    private Digester addJsonValueProducer(ValueProducer<JsonNode> piece) {
        return add(new BytesValueProducer(piece.identifier,
                   (e, c) -> piece.produce(e, c).flatMap(jsonNode ->
                           jsonNode.isValueNode()
                               ? Optional.of(ByteBuffer.wrap(jsonNode.asText().getBytes(StandardCharsets.UTF_8)))
                               : Optional.empty())));
    }

    public Digester add(final String text) {
        return add(new BytesValueProducer('"' + text + '"',
                                          (e, c) -> Optional.of(ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)))));
    }

    public static Digester create(final String algorithm) {
        return new Digester(String.format("digester(\"%s\")", algorithm), algorithm);
    }

    private static class BytesValueProducer extends PrimitiveValueProducer<ByteBuffer> {
        BytesValueProducer(final String readableName, final FieldSupplier<ByteBuffer> supplier) {
            super(readableName, ByteBuffer.class, supplier, true);
        }

        @Override
        Optional<ValidationError> validateTypes(final Schema.Field target) {
            return DslRecordMapping.validateTrivialUnion(target.schema(),
                                                         x -> x.getType() == Schema.Type.BYTES,
                                                         "only 'bytes' are supported");
        }
    }
}
