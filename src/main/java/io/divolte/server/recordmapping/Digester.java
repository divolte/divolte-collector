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
import com.google.common.collect.Streams;
import com.google.common.reflect.TypeToken;
import io.divolte.server.DivolteEvent;
import io.divolte.server.recordmapping.DslRecordMapping.ValueProducer;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@ParametersAreNonnullByDefault
public final class Digester<T> {
    private final String digestIdentifier;
    private final Supplier<T> digestFactory;
    private final Function<T,Consumer<ByteBuffer>> digestUpdaterFactory;
    private final Function<T,Supplier<byte[]>> digestFinalizerFactory;
    private List<ValueProducer<ByteBuffer>> digestPieces = new ArrayList<>();

    private Digester(final String identifier,
                     final Supplier<T> digestFactory,
                     final Function<T,Consumer<ByteBuffer>> digestUpdater,
                     final Function<T,Supplier<byte[]>> digestFinalizer) {
        digestIdentifier = Objects.requireNonNull(identifier);
        this.digestFactory = Objects.requireNonNull(digestFactory);
        this.digestUpdaterFactory = Objects.requireNonNull(digestUpdater);
        this.digestFinalizerFactory = Objects.requireNonNull(digestFinalizer);
    }

    public ValueProducer<ByteBuffer> result() {
        final String identifier =
            digestPieces.stream()
                        .map(s -> String.format(".add(%s)", s))
                        .collect(Collectors.joining("", digestIdentifier, ".result()"));
        return new BytesValueProducer(identifier, this::calculateDigest);
    }

    private Optional<ByteBuffer> calculateDigest(final DivolteEvent e, final Map<String,Optional<?>> context) {
        final T messageDigest = digestFactory.get();
        final Consumer<ByteBuffer> digestUpdater = digestUpdaterFactory.apply(messageDigest);
        final Supplier<byte[]> digestFinalizer = digestFinalizerFactory.apply(messageDigest);
        digestPieces.stream()
                    .map(x -> x.produce(e, context))
                    .flatMap(Streams::stream)
                    .forEachOrdered(digestUpdater);
        return Optional.of(ByteBuffer.wrap(digestFinalizer.get()));
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
        } else if (producerType.isSubtypeOf(ByteBuffer.class)) {
            @SuppressWarnings("unchecked")
            final ValueProducer<ByteBuffer> bytesPiece = (ValueProducer<ByteBuffer>) piece;
            return addPiece(bytesPiece);
        }
        throw new SchemaMappingException("Cannot digest value of type: %s", producerType);
    }

    private Digester addPiece(final ValueProducer<ByteBuffer> piece) {
        digestPieces.add(piece);
        return this;
    }

    private Digester addStringValueProducer(final ValueProducer<String> piece) {
        return addPiece(new BytesValueProducer(piece.identifier,
                        (e, c) -> piece.produce(e, c).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))));
    }

    private Digester addJsonValueProducer(ValueProducer<JsonNode> piece) {
        return addPiece(new BytesValueProducer(piece.identifier,
                        (e, c) -> piece.produce(e, c).flatMap(jsonNode ->
                                jsonNode.isValueNode()
                                    ? Optional.of(ByteBuffer.wrap(jsonNode.asText().getBytes(StandardCharsets.UTF_8)))
                                    : Optional.empty())));
    }

    public Digester add(final String text) {
        return addPiece(new BytesValueProducer('"' + text + '"',
                                               (e, c) -> Optional.of(ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)))));
    }

    public static Digester create(final String algorithm) {
        final String identifier = String.format("digester(\"%s\")", algorithm);
        // Check the algorithm is valid.
        try {
            MessageDigest.getInstance(algorithm);
        } catch (final NoSuchAlgorithmException e) {
            final SchemaMappingException exception = new SchemaMappingException("Algorithm not supported for digester: %s", identifier);
            exception.initCause(e);
            throw exception;
        }

        return new Digester<>(
            identifier,
            () -> {
                try {
                    return MessageDigest.getInstance(algorithm);
                } catch (final NoSuchAlgorithmException e) {
                    throw new IllegalStateException("Cannot instantiate digester for algorithm: " + algorithm, e);
                }
            },
            md -> md::update,
            md -> md::digest);
    }

    public static Digester<?> create(final String algorithm, final String seed) {
        final String identifier = String.format("digester(\"%s\", \"%s\")", algorithm, seed);
        // Check we can hash with the supplied algorithm, and convert the seed into a password that we use
        // when producing values.
        final String macAlgorithm = "hmac" + algorithm.replace("-", "");
        final SecretKey seedKey;
        try {
            final Mac mac = Mac.getInstance(macAlgorithm);
            // For HMAC the desired key length is the same as the output length.
            seedKey = deriveKey(macAlgorithm, seed, mac.getMacLength());
            mac.init(seedKey);
        } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
            final SchemaMappingException exception = new SchemaMappingException("Algorithm not supported for digester: %s", identifier);
            exception.initCause(e);
            throw exception;
        }

        return new Digester<>(
            identifier,
            () -> {
                try {
                    final Mac mac = Mac.getInstance(macAlgorithm);
                    mac.init(seedKey);
                    return mac;
                } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
                    throw new IllegalStateException("Cannot instantiate digester for algorithm: " + algorithm, e);
                }
            },
            m -> m::update,
            m -> m::doFinal);
    }

    // Normally salt should be randomised. In our use-case though a single salt value is okay.
    // (It's really serving as a "personalisation" vector.)
    private static final byte[] STATIC_SALT =
        { 'd', 'i', 'v', 'o', 'l', 't', 'e',
          (byte) 0xb6, (byte) 0x87, (byte) 0xe2, (byte) 0xd9, (byte) 0xaa, (byte) 0x06, (byte) 0x03, (byte) 0x72 };

    private static SecretKey deriveKey(final String macAlgorithm, final String seed, final int keyLength)
            throws NoSuchAlgorithmException {
        final SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBKDF2With" + macAlgorithm);
        final KeySpec keySpec = new PBEKeySpec(seed.toCharArray(), STATIC_SALT, 100000, keyLength);
        try {
            return keyFactory.generateSecret(keySpec);
        } catch (final InvalidKeySpecException e) {
            throw new IllegalArgumentException("Unable to initialise using the supplied seed: " + seed, e);
        }
    }
}
