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
import groovy.lang.GString;
import io.divolte.server.recordmapping.DslRecordMapping.ValueProducer;

import javax.annotation.ParametersAreNonnullByDefault;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@ParametersAreNonnullByDefault
public final class Joiner {
    public static final Joiner DEFAULT_JOINER = new Joiner();

    private final Optional<String> separator;
    private final Optional<String> prefix;
    private final Optional<String> suffix;

    private Joiner() {
        this(Optional.empty(), Optional.empty(), Optional.empty());
    }

    private Joiner(final Optional<String> prefix,
                   final Optional<String> suffix,
                   final Optional<String> separator) {
        this.prefix = Objects.requireNonNull(prefix);
        this.suffix = Objects.requireNonNull(suffix);
        this.separator = Objects.requireNonNull(separator);
    }

    public Joiner on(final String newSeparator) {
        return on(Optional.of(newSeparator));
    }

    public Joiner on(final Optional<String> newSeparator) {
        return new Joiner(prefix, suffix, newSeparator);
    }

    public Joiner withPrefix(final String newPrefix) {
        return withPrefix(Optional.of(newPrefix));
    }

    public Joiner withPrefix(final Optional<String> newPrefix) {
        return new Joiner(newPrefix, suffix, separator);
    }

    public Joiner withSuffix(final String newSuffix) {
        return withSuffix(Optional.of(newSuffix));
    }

    public Joiner withSuffix(final Optional<String> newSuffix) {
        return new Joiner(prefix, newSuffix, separator);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("joiner");
        separator.ifPresent(s -> builder.append(".on(\"").append(s).append('"'));
        prefix.ifPresent(s -> builder.append(".withPrefix(\"").append(s).append('"'));
        suffix.ifPresent(s -> builder.append(".withSuffix(\"").append(s).append('"'));
        return builder.toString();
    }

    @SafeVarargs
    public final ValueProducer<String> join(final ValueProducer<String>... producers) {
        final String identifier =
            toString() + ".join(" + Arrays.stream(producers)
                                          .map(p -> p.identifier)
                                          .collect(Collectors.joining(",", "concat(", ")")) + ')';
        return new DslRecordMapping.PrimitiveValueProducer<>(identifier, String.class,
            (e,c) -> {
                final Iterator<String> values =
                    Arrays.stream(producers)
                          .map(p -> p.produce(e, c))
                          .filter(Optional::isPresent)
                          .map(Optional::get)
                          .iterator();
                final Optional<String> joinedString;
                if (values.hasNext()) {
                    final StringBuilder builder = new StringBuilder();
                    prefix.ifPresent(builder::append);
                    builder.append(values.next());
                    while (values.hasNext()) {
                        separator.ifPresent(builder::append);
                        builder.append(values.next());
                    }
                    suffix.ifPresent(builder::append);
                    joinedString = Optional.of(builder.toString());
                } else {
                    joinedString = Optional.empty();
                }
                return joinedString;
            }, true);
    }

    public ValueProducer<String> join(final Object... values) {
        @SuppressWarnings("unchecked")
        final ValueProducer<String>[] checkedProducers =
            Arrays.stream(values)
                  .map(Joiner::coerce)
                  .toArray(ValueProducer[]::new);
        return join(checkedProducers);
    }

    private static Class<?> getProducerType(final ValueProducer<?> producer) {
        final TypeToken<? extends ValueProducer> token = TypeToken.of(producer.getClass());
        final ParameterizedType type = (ParameterizedType) token.getSupertype(ValueProducer.class).getType();
        final Type[] parameters = type.getActualTypeArguments();
        return (Class<?>) parameters[0];
    }

    @SuppressWarnings("unchecked")
    private static ValueProducer<String> coerce(final Object value) {
        final Optional<ValueProducer<String>> producer;
        if (value instanceof ValueProducer) {
            final ValueProducer<?> valueProducer = (ValueProducer) value;
            final Class<?> producerType = getProducerType(valueProducer);
            if (String.class.isAssignableFrom(producerType)) {
                producer = Optional.of((ValueProducer<String>) value);
            } else if (JsonNode.class.isAssignableFrom(producerType)) {
                producer = Optional.of(new DslRecordMapping.PrimitiveValueProducer<>(valueProducer.identifier,
                                                                                     String.class,
                            (e, c) -> valueProducer.produce(e, c).flatMap(JacksonSupport::toString)));
            } else {
                producer = Optional.empty();
            }
        } else if (value instanceof String || value instanceof GString) {
            final String literal = value.toString();
            final Optional<String> producedValue = Optional.of(literal);
            producer = Optional.of(new DslRecordMapping.PrimitiveValueProducer<>('"' + literal.replace("\"", "\\\""),
                                                                                 String.class,
                                                                                 (e, c) -> producedValue));
        } else {
            producer = Optional.empty();
        }
        return producer.orElseThrow(() -> new IllegalArgumentException("Only strings can be joined"));
    }
}
