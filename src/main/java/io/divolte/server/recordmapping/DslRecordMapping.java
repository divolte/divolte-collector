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
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Joiner;
import com.google.common.collect.*;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.maxmind.geoip2.model.AbstractCityResponse;
import com.maxmind.geoip2.model.AbstractCountryResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import io.divolte.server.DivolteEvent;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.ip2geo.LookupService.ClosedServiceException;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.Cookie;
import io.undertow.util.Headers;
import net.sf.uadetector.ReadableUserAgent;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bouncycastle.jcajce.provider.digest.SHA3.DigestSHA3;
import org.bouncycastle.jcajce.provider.digest.SHA3.Digest256;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import static io.divolte.server.IncomingRequestProcessor.DUPLICATE_EVENT_KEY;

@ParametersAreNonnullByDefault
@NotThreadSafe
public final class DslRecordMapping {
    private static final Map<Class<?>,Type> COMPATIBLE_PRIMITIVES =
            ImmutableMap.<Class<?>,Type>builder()
                        .put(Boolean.class, Type.BOOLEAN)
                        .put(String.class, Type.STRING)
                        .put(Float.class, Type.FLOAT)
                        .put(Double.class, Type.DOUBLE)
                        .put(Integer.class, Type.INT)
                        .put(Long.class, Type.LONG)
                        .build();

    private static final Configuration JSON_PATH_CONFIGURATION =
            Configuration.builder()
                    .options(Option.SUPPRESS_EXCEPTIONS)
                    .jsonProvider(new JacksonJsonNodeJsonProvider())
                    .build();

    private final Schema schema;
    private final ArrayDeque<ImmutableList.Builder<MappingAction>> stack;

    private final UserAgentParserAndCache uaParser;
    private final Optional<LookupService> geoIpService;
    private final AvroGenericRecordMapper jsonMapper = JacksonSupport.createAvroMapper();

    public DslRecordMapping(final Schema schema, final UserAgentParserAndCache uaParser, final Optional<LookupService> geoIpService) {
        this.schema = Objects.requireNonNull(schema);
        this.uaParser = Objects.requireNonNull(uaParser);
        this.geoIpService = Objects.requireNonNull(geoIpService);

        stack = new ArrayDeque<>();
        stack.add(ImmutableList.builder());
    }

    /*
     * Standard actions
     */
    public <T> void map(final String fieldName, final ValueProducer<T> producer) {
        final Field field = schema.getField(fieldName);
        if (field == null) {
            throw new SchemaMappingException("Field %s does not exist in Avro schema; error in mapping %s onto %s", fieldName, producer.identifier, fieldName);
        }
        final Optional<ValidationError> validationError = producer.validateTypes(field);
        if (validationError.isPresent()) {
            throw new SchemaMappingException("Cannot map the result of %s onto field %s: %s",
                                             producer.identifier, fieldName, validationError.get().message);
        }
        stack.getLast().add((e,c,r) -> {
            producer.produce(e,c)
                    .flatMap(v -> producer.mapToGenericRecord(v, field.schema()))
                    .ifPresent(v -> r.set(field, v));
            return MappingAction.MappingResult.CONTINUE;
        });
    }

    public <T> void map(final String fieldName, final T literal) {
        if (!COMPATIBLE_PRIMITIVES.containsKey(literal.getClass())) {
            throw new SchemaMappingException("Type error. Cannot map literal %s of type %s. Only primitive types are allowed.", literal.toString(), literal.getClass());
        }

        final Field field = schema.getField(fieldName);
        if (field == null) {
            throw new SchemaMappingException("Field %s does not exist in Avro schema; error in mapping %s onto %s", fieldName, literal, fieldName);
        }

        final Optional<Schema> targetSchema = unpackNullableUnion(field.schema());
        if (!targetSchema.map((s) -> s.getType() == COMPATIBLE_PRIMITIVES.get(literal.getClass())).orElse(false)) {
            throw new SchemaMappingException("Type mismatch. Cannot map literal %s of type %s onto a field of type %s (type of value and schema of field do not match).", literal.toString(), literal.getClass(), field.schema());
        }

        stack.getLast().add((e,c,r) -> {
            r.set(field, literal);
            return MappingAction.MappingResult.CONTINUE;
        });
    }

    /*
     * Higher order actions
     */
    public void when(final ValueProducer<Boolean> condition, final Runnable closure) {
        stack.add(ImmutableList.builder());
        closure.run();

        final List<MappingAction> actions = stack.removeLast().build();
        stack.getLast().add((e,c,r) -> {
           if (condition.produce(e, c).orElse(false)) {
               for (final MappingAction action : actions) {
                   switch (action.perform(e, c, r)) {
                   case EXIT:
                       return MappingAction.MappingResult.EXIT;
                   case STOP:
                       return MappingAction.MappingResult.STOP;
                   case CONTINUE:
                   }
               }
           }
           return MappingAction.MappingResult.CONTINUE;
        });
    }

    public void section(final Runnable closure) {
        stack.add(ImmutableList.builder());
        closure.run();

        final List<MappingAction> actions = stack.removeLast().build();
        stack.getLast().add((e,c,r) -> {
           for (final MappingAction action : actions) {
               switch (action.perform(e, c, r)) {
               case EXIT:
                   return MappingAction.MappingResult.CONTINUE;
               case STOP:
                   return MappingAction.MappingResult.STOP;
               case CONTINUE:
               }
           }
           return MappingAction.MappingResult.CONTINUE;
        });
    }

    /*
     * Short circuit actions
     */
    public void stop() {
        stack.getLast().add((e,c,r) -> MappingAction.MappingResult.STOP);
    }

    public void exitWhen(final ValueProducer<Boolean> condition) {
        stack.getLast().add(
                (e,c,r) -> condition.produce(e, c)
                                      .map((b) -> b ? MappingAction.MappingResult.EXIT : MappingAction.MappingResult.CONTINUE)
                                      .orElse(MappingAction.MappingResult.CONTINUE));
    }

    public void exit() {
        stack.getLast().add((e,c,r) -> MappingAction.MappingResult.EXIT);
    }

    /*
     * The mapping result, used by the record mapper.
     */
    List<MappingAction> actions() {
        return stack.getLast().build();
    }

    /*
     * Casting and conversion
     */
    public ValueProducer<Integer> toInt(final ValueProducer<String> source) {
        return new PrimitiveValueProducer<>(
                "parse(" + source.identifier + " to int32)",
                Integer.class,
                (e,c) -> source.produce(e, c).map(Ints::tryParse));
    }

    public ValueProducer<Long> toLong(final ValueProducer<String> source) {
        return new PrimitiveValueProducer<>(
                "parse(" + source.identifier + " to int64)",
                Long.class,
                (e,c) -> source.produce(e, c).map(Longs::tryParse));
    }

    public ValueProducer<Float> toFloat(final ValueProducer<String> source) {
        return new PrimitiveValueProducer<>(
                "parse(" + source.identifier + " to fp32)",
                Float.class,
                (e,c) -> source.produce(e, c).map(Floats::tryParse));
    }

    public ValueProducer<Double> toDouble(final ValueProducer<String> source) {
        return new PrimitiveValueProducer<>(
                "parse(" + source.identifier + " to fp64)",
                Double.class,
                (e,c) -> source.produce(e, c).map(Doubles::tryParse));
    }

    public BooleanValueProducer toBoolean(final ValueProducer<String> source) {
        return new BooleanValueProducer(
                "parse(" + source.identifier + " to bool)",
                (e,c) -> source.produce(e, c).map(Boolean::parseBoolean));
    }

    /*
     * Simple field mappings
     */
    public ValueProducer<String> location() {
        return browserEventValueProducer("location()", String.class, e -> e.location);
    }

    public ValueProducer<String> referer() {
        return browserEventValueProducer("referer()", String.class, e -> e.referer);
    }

    public ValueProducer<String> eventType() {
        return new PrimitiveValueProducer<>("eventType()", String.class, (e,c) -> e.eventType);
    }

    public BooleanValueProducer firstInSession() {
        return new BooleanValueProducer("firstInSession()", (e,c) -> Optional.of(e.firstInSession));
    }

    public BooleanValueProducer corrupt() {
        return new BooleanValueProducer("corrupt()", (e,c) -> Optional.of(e.corruptEvent));
    }

    public BooleanValueProducer duplicate() {
        return new BooleanValueProducer("duplicate()", (e,c) -> Optional.ofNullable(e.exchange.getAttachment(DUPLICATE_EVENT_KEY)));
    }

    public ValueProducer<Long> timestamp() {
        return new PrimitiveValueProducer<>("timestamp()", Long.class, (e,c) -> Optional.of(e.requestStartTime.toEpochMilli()));
    }

    public ValueProducer<Long> clientTimestamp() {
        return new PrimitiveValueProducer<>("clientTimestamp()", Long.class, (e,c) -> Optional.of(e.clientTime.toEpochMilli()));
    }

    public StringValueProducer remoteHost() {
        return new StringValueProducer("remoteHost()", (e,c) -> Optional.ofNullable(e.exchange.getSourceAddress()).map(InetSocketAddress::getHostString));
    }

    public ValueProducer<Integer> viewportPixelWidth() {
        return browserEventValueProducer("viewportPixelWidth()", Integer.class, e -> e.viewportPixelWidth);
    }

    public ValueProducer<Integer> viewportPixelHeight() {
        return browserEventValueProducer("viewportPixelHeight()", Integer.class, e -> e.viewportPixelHeight);
    }

    public ValueProducer<Integer> screenPixelWidth() {
        return browserEventValueProducer("screenPixelWidth()", Integer.class, e -> e.screenPixelWidth);
    }

    public ValueProducer<Integer> screenPixelHeight() {
        return browserEventValueProducer("screenPixelHeight()", Integer.class, e -> e.screenPixelHeight);
    }

    public ValueProducer<Integer> devicePixelRatio() {
        return browserEventValueProducer("devicePixelRatio()", Integer.class, e -> e.devicePixelRatio);
    }

    public StringValueProducer partyId() {
        return new StringValueProducer("partyId()", (e,c) -> Optional.of(e.partyId.value));
    }

    public StringValueProducer sessionId() {
        return new StringValueProducer("sessionId()", (e,c) -> Optional.of(e.sessionId.value));
    }

    public ValueProducer<String> pageViewId() {
        return browserEventValueProducer("pageViewId()", String.class, e -> Optional.of(e.pageViewId));
    }

    public StringValueProducer eventId() {
        return new StringValueProducer("eventId()", (e,c) -> Optional.of(e.eventId));
    }

    /*
     * User agent mapping
     */
    public StringValueProducer userAgentString() {
        return new StringValueProducer("userAgentString()",
                                       (e, c) -> Optional.ofNullable(e.exchange.getRequestHeaders().getFirst(Headers.USER_AGENT)));
    }

    public UserAgentValueProducer userAgent() {
        return new UserAgentValueProducer(userAgentString(), uaParser);
    }

    public final class UserAgentValueProducer extends ValueProducer<ReadableUserAgent> {
        UserAgentValueProducer(final ValueProducer<String> source, final UserAgentParserAndCache parser) {
            super("userAgent()", (e, c) -> source.produce(e, c).flatMap(parser::tryParse), true);
        }

        public StringValueProducer name() {
            return new StringValueProducer(identifier + ".name()",
                                                (e,c) -> produce(e, c).map(ReadableUserAgent::getName));
        }

        public StringValueProducer family() {
            return new StringValueProducer(identifier + ".family()",
                                                (e,c) -> produce(e, c).map((ua) -> ua.getFamily().getName()));
        }

        public StringValueProducer vendor() {
            return new StringValueProducer(identifier + ".vendor()",
                                                (e,c) -> produce(e, c).map(ReadableUserAgent::getProducer));
        }

        public StringValueProducer type() {
            return new StringValueProducer(identifier + ".type()",
                                                (e,c) -> produce(e, c).map((ua) -> ua.getType().getName()));
        }

        public StringValueProducer version() {
            return new StringValueProducer(identifier + ".version()",
                                                (e,c) -> produce(e, c).map((ua) -> ua.getVersionNumber().toVersionString()));
        }

        public StringValueProducer deviceCategory() {
            return new StringValueProducer(identifier + ".deviceCategory()",
                                                (e,c) -> produce(e, c).map((ua) -> ua.getDeviceCategory().getName()));
        }

        public StringValueProducer osFamily() {
            return new StringValueProducer(identifier + ".osFamily()",
                                                (e,c) -> produce(e, c).map((ua) -> ua.getOperatingSystem().getFamily().getName()));
        }

        public StringValueProducer osVersion() {
            return new StringValueProducer(identifier + ".osVersion()",
                                                (e,c) -> produce(e, c).map((ua) -> ua.getOperatingSystem().getVersionNumber().toVersionString()));
        }

        public StringValueProducer osVendor() {
            return new StringValueProducer(identifier + ".osVendor()",
                                                (e,c) -> produce(e, c).map((ua) -> ua.getOperatingSystem().getProducer()));
        }

        @Override
        Optional<ValidationError> validateTypes(final Field target) {
            // this field cannot be directly mapped onto a Avro schema
            return validationError("cannot map a parsed user-agent directly; map one of its properties instead.", target.schema());
        }
    }

    /*
     * Regex mapping
     */
    public ValueProducer<Matcher> matcher(final ValueProducer<String> source, final String regex) {
        return new MatcherValueProducer(source, regex);
    }

    public final static class MatcherValueProducer extends ValueProducer<Matcher> {
        MatcherValueProducer(final ValueProducer<String> source, final String regex) {
            super("match(" + regex + " against " + source.identifier + ")",
                  (e, c) -> source.produce(e, c).map((s) -> Pattern.compile(regex).matcher(s)),
                  true);
        }

        public BooleanValueProducer matches() {
            return new BooleanValueProducer(identifier + ".matches()",
                                            (e,c) -> produce(e, c).map(Matcher::matches));
        }

        // Note: matches() must be called on a Matcher prior to calling group
        // In case of no match, group(...) throws an exception, in case there is
        // a match, but the group doesn't capture anything, it returns null.
        public StringValueProducer group(final int group) {
            return new StringValueProducer(identifier + ".group(" + group + ")",
                                           (e,c) -> produce(e, c).map((m) -> m.matches() ? m.group(group) : null));
        }

        public StringValueProducer group(final String group) {
            return new StringValueProducer(identifier + ".group(" + group + ")",
                                           (e,c) -> produce(e, c).map((m) -> m.matches() ? m.group(group) : null));
        }

        @Override
        Optional<ValidationError> validateTypes(final Field target) {
            // this field cannot be directly mapped onto a Avro schema
            return validationError("cannot map a regular expression match directly; map using group() or matches() instead.", target.schema());
        }
    }

    /*
     * URI parsing
     */
    public ValueProducer<URI> parseUri(final ValueProducer<String> source) {
        return new UriValueProducer(source);
    }

    public final static class UriValueProducer extends ValueProducer<URI> {
        UriValueProducer(final ValueProducer<String> source) {
            super("parse(" + source.identifier + " to uri)",
                  (e,c) -> source.produce(e, c).map((location) -> {
                        try {
                            return new URI(location);
                        } catch (final Exception ignored) {
                            // When we cannot parse as URI, leave the producer empty
                            return null;
                        }
                    }),
                  true);
        }

        public StringValueProducer path() {
            return new StringValueProducer(identifier + ".path()",
                                           (e,c) -> produce(e, c).map(URI::getPath));
        }

        public StringValueProducer rawPath() {
            return new StringValueProducer(identifier + ".rawPath()",
                                           (e,c) -> produce(e, c).map(URI::getRawPath));
        }

        public StringValueProducer scheme() {
            return new StringValueProducer(identifier + ".scheme()",
                                           (e,c) -> produce(e, c).map(URI::getScheme));
        }

        public StringValueProducer host() {
            return new StringValueProducer(identifier + ".host()",
                                           (e,c) -> produce(e, c).map(URI::getHost));
        }

        public ValueProducer<Integer> port() {
            return new PrimitiveValueProducer<>(identifier + ".port()",
                                                Integer.class,
                                                (e,c) -> produce(e, c).map((uri) -> uri.getPort() != -1 ? uri.getPort() : null));
        }

        public StringValueProducer decodedQueryString() {
            return new StringValueProducer(identifier + ".decodedQueryString()",
                                           (e,c) -> produce(e, c).map(URI::getQuery));
        }

        public StringValueProducer rawQueryString() {
            return new StringValueProducer(identifier + ".rawQueryString()",
                                           (e,c) -> produce(e, c).map(URI::getRawQuery));
        }

        public StringValueProducer decodedFragment() {
            return new StringValueProducer(identifier + ".decodedFragment()",
                                           (e,c) -> produce(e, c).map(URI::getFragment));
        }

        public StringValueProducer rawFragment() {
            return new StringValueProducer(identifier + ".rawFragment()",
                                           (e,c) -> produce(e, c).map(URI::getRawFragment));
        }

        public QueryStringValueProducer query() {
            return new QueryStringValueProducer(rawQueryString());
        }

        @Override
        Optional<ValidationError> validateTypes(final Field target) {
            return validationError("cannot map an URI directly; map one of its properties.", target.schema());
        }
    }

    public final static class QueryStringValueProducer extends ValueProducer<Map<String,List<String>>> {
        QueryStringValueProducer(final StringValueProducer source) {
            super("parse (" + source.identifier + " to querystring)",
                  (e,c) -> source.produce(e, c).map(QueryStringParser::parseQueryString),
                  true);
        }

        public StringValueProducer value(final String key) {
            // Note that we do not check for the empty list, as is could not exist; if the key is in the map, there is at least one element in the list
            return new StringValueProducer(identifier + ".value(" + key + ")",
                                            (e,c) -> produce(e, c).map((qs) -> qs.get(key)).map((l) -> l.get(0)));
        }

        public ValueProducer<List<String>> valueList(final String key) {
            return new PrimitiveListValueProducer<>(identifier + ".valueList(" + key + ")",
                                                String.class,
                                                (e,c) -> produce(e, c).map((qs) -> qs.get(key)));
        }

        @Override
        Optional<ValidationError> validateTypes(final Field target) {
            return validateTrivialUnion(target.schema(),
                                        s -> s.getType() == Type.MAP &&
                                             s.getValueType().getType() == Type.ARRAY &&
                                             s.getValueType().getElementType().getType() == Type.STRING,
                                        "query strings can only be mapped to an Avro map where the values are arrays of strings");
        }
    }

    /*
     * Cookie mapping
     */
    public StringValueProducer cookie(final String name) {
        return new StringValueProducer("cookie(" + name + ")",
                                       (e,c) -> Optional.ofNullable(e.exchange.getRequestCookies().get(name)).map(Cookie::getValue));
    }

    /*
     * Custom event parameter mapping
     */
    public final class EventParameterValueProducer extends JsonValueProducer {
        EventParameterValueProducer() {
            super("eventParameters()", (e,c) -> e.eventParametersProducer.get(), true);
        }

        public StringValueProducer value(final String name) {
            return new StringValueProducer(
                    identifier + ".value(" + name + ")",
                    (e,c) -> produce(e, c).map(json -> json.path(name).asText()));
        }

        public ValueProducer<JsonNode> path(final String path) {
            final JsonPath jsonPath = JsonPath.compile(path);
            return new JsonValueProducer(
                    identifier + ".path(" + path + ')',
                    (e,c) -> produce(e, c).map(json -> jsonPath.read(json, JSON_PATH_CONFIGURATION)),
                    false);
        }
    }

    @ParametersAreNonnullByDefault
    private class JsonValueProducer extends ValueProducer<JsonNode> {
        private final Logger logger = LoggerFactory.getLogger(JsonValueProducer.class);

        protected JsonValueProducer(final String identifier,
                                    final FieldSupplier<JsonNode> supplier,
                                    final boolean memoize) {
            super(identifier, supplier, memoize);
        }

        @Override
        Optional<ValidationError> validateTypes(final Field target) {
            /*
             * We can do some basic validation here because there are
             * some Avro schemas (e.g non-trivial unions) that we don't
             * support.
             */
            return jsonMapper.checkValid(target.schema());
        }

        @Override
        Optional<Object> mapToGenericRecord(final JsonNode value,
                                            final Schema schema) throws SchemaMappingException {
            try {
                return Optional.ofNullable(jsonMapper.read(value, schema));
            } catch (final IOException e) {
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("Error mapping JSON value %s to schema: %s", value, schema), e);
                }
                return Optional.empty();
            }
        }
    }

    public EventParameterValueProducer eventParameters() {
        return new EventParameterValueProducer();
    }

    /*
     * Remove this at some point. It is not documented, but used in mappings
     * on some installations.
     */
    @Deprecated
    public StringValueProducer eventParameter(final String name) {
        final EventParameterValueProducer eventParametersProducer = eventParameters();
        return new StringValueProducer("eventParameter(" + name + ")",
                                       (e,c) -> eventParametersProducer.produce(e,c)
                                                        .map(json -> json.path(name).asText()));
    }

    /*
     * Custom header mapping
     */
    public HeaderValueProducer header(final String name) {
        return new HeaderValueProducer(name);
    }

    public final static class HeaderValueProducer extends PrimitiveListValueProducer<String> {
        private final static Joiner COMMA_JOINER = Joiner.on(',');
        private final String headerName;

        HeaderValueProducer(final String headerName) {
            super("header(" + headerName + ")",
                  String.class,
                  (e,c) -> normalizedValues(e.exchange, headerName).map(x -> x.collect(Collectors.toList())));
            this.headerName = Objects.requireNonNull(headerName);
        }

        private static Optional<Stream<String>> normalizedValues(final HttpServerExchange exchange, final String headerName) {
            return Optional.ofNullable(exchange.getRequestHeaders().get(headerName))
                           .map(h -> h.stream().flatMap(HeaderValueParser::values));
        }

        public StringValueProducer first() {
            return first(identifier + ".first()");
        }

        private StringValueProducer first(final String readableName) {
            return new StringValueProducer(readableName,
                                           (e,c) -> normalizedValues(e.exchange, headerName).map(s -> s.findFirst().orElse(null)));
        }

        public StringValueProducer last() {
            return last(identifier + ".last()");
        }
        public StringValueProducer last(final String readableName) {
            return new StringValueProducer(readableName,
                                           (e,c) -> normalizedValues(e.exchange, headerName).map(s -> Streams.findLast(s).orElse(null)));
        }

        // Find the x'th element from the end.
        // Note that indexes here are zero-based: 0 means the last element, 1 the second-last, etc.
        private static <T> Optional<T> findFromEnd(final Stream<T> stream, final int index) {
            // To do this we'll pump the stream through a FIFO bounded buffer.
            // When it finishes, the head of the buffer is the x-th last element.
            final int bufferSize = index + 1;
            final EvictingQueue<T> buffer = EvictingQueue.create(bufferSize);
            stream.forEachOrdered(buffer::add);
            // If we didn't fill the buffer there weren't enough elements in the stream.
            return buffer.size() < bufferSize ? Optional.empty() : Optional.ofNullable(buffer.peek());
        }

        public StringValueProducer get(final int index) {
            final StringValueProducer producer;
            final String readableName = identifier + ".get(" + index + ')';
            switch (index) {
                case -1:
                    producer = last(readableName);
                    break;
                case 0:
                    producer = first(readableName);
                    break;
                default:
                    final ValueProducer.FieldSupplier<String> supplier = 0 < index
                        ? (e,c) -> normalizedValues(e.exchange, headerName)
                                    .map(s -> s.skip(index).findFirst().orElse(null))
                        : (e,c) -> normalizedValues(e.exchange, headerName)
                                    .map(s -> findFromEnd(s, -index - 1).orElse(null));
                    producer = new StringValueProducer(readableName, supplier);
            }
            return producer;
        }

        public StringValueProducer commaSeparated() {
            return new StringValueProducer(identifier + ".commaSeparated()",
                                           (e,c) -> produce(e, c).map(COMMA_JOINER::join));
        }
    }

    /*
     * IP to geo mapping
     */
    public GeoIpValueProducer ip2geo(final ValueProducer<String> source) {
        final PrimitiveValueProducer<InetAddress> addressProducer =
                new PrimitiveValueProducer<>("parse(" + source.identifier + " to IP address)",
                                             InetAddress.class,
                                             (e, c) -> source.produce(e, c).flatMap(DslRecordMapping::tryParseIpv4),
                                             true);
        return new GeoIpValueProducer(addressProducer, verifyAndReturnLookupService());
    }

    public GeoIpValueProducer ip2geo() {
        final PrimitiveValueProducer<InetAddress> addressProducer =
                new PrimitiveValueProducer<>("<remote host IP>",
                                             InetAddress.class,
                                             (e,c) -> Optional.ofNullable(e.exchange.getSourceAddress()).map(InetSocketAddress::getAddress));
        return new GeoIpValueProducer(addressProducer, verifyAndReturnLookupService());
    }

    private LookupService verifyAndReturnLookupService() {
        return geoIpService.orElseThrow(() -> new SchemaMappingException("Attempt to use a ip2geo mapping, while the ip2geo lookup database is not configured."));
    }

    public final static class GeoIpValueProducer extends ValueProducer<CityResponse> {
        GeoIpValueProducer(final ValueProducer<InetAddress> source, final LookupService service) {
            super("ip2geo(" + source.identifier + ")",
                  (e,c) -> source.produce(e, c).flatMap((address) -> {
                        try {
                            return service.lookup(address);
                        } catch (final ClosedServiceException ex) {
                            return Optional.empty();
                        }
                    }),
                  true);
        }

        public ValueProducer<Integer> cityId() {
            return new PrimitiveValueProducer<>(identifier + ".cityId()",
                                           Integer.class,
                                           (e,c) -> produce(e, c).map(AbstractCityResponse::getCity).map(City::getGeoNameId));
        }

        public StringValueProducer cityName() {
            return new StringValueProducer(identifier + ".cityName()",
                                           (e,c) -> produce(e, c).map(AbstractCityResponse::getCity).map(City::getName));
        }

        public StringValueProducer continentCode() {
            return new StringValueProducer(identifier + ".continentCode()",
                                           (e,c) -> produce(e, c).map(AbstractCountryResponse::getContinent).map(Continent::getCode));
        }

        public ValueProducer<Integer> continentId() {
            return new PrimitiveValueProducer<>(identifier + ".continentId()",
                                                Integer.class,
                                                (e,c) -> produce(e, c).map(AbstractCountryResponse::getContinent).map(Continent::getGeoNameId));
        }

        public StringValueProducer continentName() {
            return new StringValueProducer(identifier + ".continentName()",
                                           (e,c) -> produce(e, c).map(AbstractCountryResponse::getContinent).map(Continent::getName));
        }

        public StringValueProducer countryCode() {
            return new StringValueProducer(identifier + ".countryCode()",
                                           (e,c) -> produce(e, c).map(AbstractCountryResponse::getCountry).map(Country::getIsoCode));
        }

        public ValueProducer<Integer> countryId() {
            return new PrimitiveValueProducer<>(identifier + ".countryId()",
                                                Integer.class,
                                                (e,c) -> produce(e, c).map(AbstractCountryResponse::getCountry).map(Country::getGeoNameId));
        }

        public StringValueProducer countryName() {
            return new StringValueProducer(identifier + ".countryName()",
                                           (e,c) -> produce(e, c).map(AbstractCountryResponse::getCountry).map(Country::getName));
        }

        public ValueProducer<Double> latitude() {
            return new PrimitiveValueProducer<>(identifier + ".latitude()",
                                                Double.class,
                                                (e,c) -> produce(e, c).map(AbstractCityResponse::getLocation).map(Location::getLatitude));
        }

        public ValueProducer<Double> longitude() {
            return new PrimitiveValueProducer<>(identifier + ".latitude()",
                                                Double.class,
                                                (e,c) -> produce(e, c).map(AbstractCityResponse::getLocation).map(Location::getLongitude));
        }

        public ValueProducer<Integer> metroCode() {
            return new PrimitiveValueProducer<>(identifier + ".metroCode()",
                                                Integer.class,
                                                (e,c) -> produce(e, c).map(AbstractCityResponse::getLocation).map(Location::getMetroCode));
        }

        public StringValueProducer timeZone() {
            return new StringValueProducer(identifier + ".timeZone()",
                                           (e,c) -> produce(e, c).map(AbstractCityResponse::getLocation).map(Location::getTimeZone));
        }

        public StringValueProducer mostSpecificSubdivisionCode() {
            return new StringValueProducer(identifier + ".mostSpecificSubdivisionCode()",
                                           (e,c) -> produce(e, c).map(AbstractCityResponse::getMostSpecificSubdivision).map(Subdivision::getIsoCode));
        }

        public ValueProducer<Integer> mostSpecificSubdivisionId() {
            return new PrimitiveValueProducer<>(identifier + ".mostSpecificSubdivisionId()",
                                                Integer.class,
                                                (e,c) -> produce(e, c).map(AbstractCityResponse::getMostSpecificSubdivision).map(Subdivision::getGeoNameId));
        }

        public StringValueProducer mostSpecificSubdivisionName() {
            return new StringValueProducer(identifier + ".mostSpecificSubdivisionName()",
                                           (e,c) -> produce(e, c).map(AbstractCityResponse::getMostSpecificSubdivision).map(Subdivision::getName));
        }

        public StringValueProducer postalCode() {
            return new StringValueProducer(identifier + ".postalCode()",
                                           (e,c) -> produce(e, c).map(AbstractCityResponse::getPostal).map(Postal::getCode));
        }

        public StringValueProducer registeredCountryCode() {
            return new StringValueProducer(identifier + ".registeredCountryCode()",
                                           (e,c) -> produce(e, c).map(AbstractCountryResponse::getRegisteredCountry).map(Country::getIsoCode));
        }

        public ValueProducer<Integer> registeredCountryId() {
            return new PrimitiveValueProducer<>(identifier + ".registeredCountryId()",
                                                Integer.class,
                                                (e,c) -> produce(e, c).map(AbstractCountryResponse::getRegisteredCountry).map(Country::getGeoNameId));
        }

        public StringValueProducer registeredCountryName() {
            return new StringValueProducer(identifier + ".registeredCountryName()",
                                           (e,c) -> produce(e, c).map(AbstractCountryResponse::getRegisteredCountry).map(Country::getName));
        }

        public StringValueProducer representedCountryCode() {
            return new StringValueProducer(identifier + ".representedCountryCode()",
                                           (e,c) -> produce(e, c).map(AbstractCountryResponse::getRepresentedCountry).map(Country::getIsoCode));
        }

        public ValueProducer<Integer> representedCountryId() {
            return new PrimitiveValueProducer<>(identifier + ".representedCountryId()",
                                                Integer.class,
                                                (e,c) -> produce(e, c).map(AbstractCountryResponse::getRepresentedCountry).map(Country::getGeoNameId));
        }

        public StringValueProducer representedCountryName() {
            return new StringValueProducer(identifier + ".representedCountryName()",
                                           (e,c) -> produce(e, c).map(AbstractCountryResponse::getRepresentedCountry).map(Country::getName));
        }

        public ValueProducer<List<String>> subdivisionCodes() {
            return new PrimitiveListValueProducer<>(identifier + ".subdivisionCodes()",
                                                String.class,
                                                (e,c) -> produce(e, c).map(AbstractCityResponse::getSubdivisions).map((l) -> Lists.transform(l, Subdivision::getIsoCode)));
        }

        public ValueProducer<List<Integer>> subdivisionIds() {
            return new PrimitiveListValueProducer<>(identifier + ".subdivisionIds()",
                                                Integer.class,
                                                (e,c) -> produce(e, c).map(AbstractCityResponse::getSubdivisions).map((l) -> Lists.transform(l, Subdivision::getGeoNameId)));
        }

        public ValueProducer<List<String>> subdivisionNames() {
            return new PrimitiveListValueProducer<>(identifier + ".subdivisionNames()",
                                                String.class,
                                                (e,c) -> produce(e, c).map(AbstractCityResponse::getSubdivisions).map((l) -> Lists.transform(l, Subdivision::getName)));
        }

        public ValueProducer<Integer> autonomousSystemNumber() {
            return new PrimitiveValueProducer<>(identifier + ".autonomousSystemNumber()",
                                                Integer.class,
                                                (e,c) -> produce(e, c).map(AbstractCountryResponse::getTraits).map(Traits::getAutonomousSystemNumber));
        }

        public StringValueProducer autonomousSystemOrganization() {
            return new StringValueProducer(identifier + ".autonomousSystemOrganization()",
                                           (e,c) -> produce(e, c).map(AbstractCountryResponse::getTraits).map(Traits::getAutonomousSystemOrganization));
        }

        public StringValueProducer domain() {
            return new StringValueProducer(identifier + ".domain()",
                                           (e,c) -> produce(e, c).map(AbstractCountryResponse::getTraits).map(Traits::getDomain));
        }

        public StringValueProducer isp() {
            return new StringValueProducer(identifier + ".isp()",
                                           (e,c) -> produce(e, c).map(AbstractCountryResponse::getTraits).map(Traits::getIsp));
        }

        public StringValueProducer organisation() {
            return new StringValueProducer(identifier + ".organisation()",
                                           (e,c) -> produce(e, c).map(AbstractCountryResponse::getTraits).map(Traits::getOrganization));
        }

        @Deprecated
        @SuppressWarnings("deprecation")
        public BooleanValueProducer anonymousProxy() {
            return new BooleanValueProducer(identifier + ".anonymousProxy()",
                                            (e,c) -> produce(e, c).map(AbstractCountryResponse::getTraits).map(Traits::isAnonymousProxy));
        }

        @Deprecated
        @SuppressWarnings("deprecation")
        public BooleanValueProducer satelliteProvider() {
            return new BooleanValueProducer(identifier + ".satelliteProvider()",
                                            (e,c) -> produce(e, c).map(AbstractCountryResponse::getTraits).map(Traits::isSatelliteProvider));
        }

        @Override
        Optional<ValidationError> validateTypes(final Field target) {
            return validationError("cannot map a GeoIP lookup result directly; map one of its properties instead.", target.schema());
        }
    }

    /*
     * Handy when using something other than the remote host as IP address (e.g. custom headers set by a load balancer).
     * We force the address to be in the numeric IPv4 scheme, to prevent name resolution and IPv6.
     */
    private static Optional<InetAddress> tryParseIpv4(final String ip) {
        final byte[] result = new byte[4];
        final String[] parts = StringUtils.split(ip, '.');
        if (parts.length != 4) {
            return Optional.empty();
        }
        try {
            for (int c = 0; c < 4; c++) {
                final int x = Integer.parseInt(parts[c]);
                if (x < 0x00 || x > 0xff) {
                    return Optional.empty();
                }
                result[c] = (byte) (x & 0xff);
            }
        } catch(final NumberFormatException nfe) {
            return Optional.empty();
        }

        try {
            return Optional.of(InetAddress.getByAddress(result));
        } catch (final UnknownHostException e) {
            return Optional.empty();
        }
    }

    private static Optional<Schema> unpackNullableUnion(final Schema source) {
        if (source.getType() == Type.UNION) {
            if (source.getTypes().size() != 2) {
                return Optional.empty();
            } else {
                return source.getTypes().stream().filter((t) -> t.getType() != Type.NULL).findFirst();
            }
        } else {
            return Optional.of(source);
        }
    }

    @FunctionalInterface
    private interface BrowserFieldSupplier<T> {
        Optional<T> apply(DivolteEvent.BrowserEventData event);
    }

    private static <T> ValueProducer<T> browserEventValueProducer(final String readableName,
                                                                  final Class<T> type,
                                                                  final BrowserFieldSupplier<T> fieldSupplier) {
        return new PrimitiveValueProducer<>(readableName, type,
                                            (e,c) -> e.browserEventData.flatMap(fieldSupplier::apply));
    }

    @ParametersAreNonnullByDefault
    public static abstract class ValueProducer<T> {

        protected interface FieldSupplier<T> {
            Optional<T> apply(DivolteEvent eventData,
                              Map<String,Optional<?>> context);
        }

        protected final String identifier;
        private final FieldSupplier<T> supplier;
        private final boolean memoize;

        ValueProducer(final String identifier, final FieldSupplier<T> supplier, final boolean memoize) {
            this.identifier = Objects.requireNonNull(identifier);
            this.supplier   = Objects.requireNonNull(supplier);
            this.memoize    = memoize;
        }

        ValueProducer(final String identifier, final FieldSupplier<T> supplier) {
            this(identifier, supplier, false);
        }

        @SuppressWarnings("unchecked")
        final Optional<T> produce(final DivolteEvent divolteEvent,
                                  final Map<String,Optional<?>> context) {
            final Optional<T> result;
            if (memoize) {
                // We can't use context.computeIfAbsent(): in Java 9 they've made it clear that the supplier
                // isn't allowed to modify the mapping being modified, and doing so will (often) throw
                // ConcurrentModificationException.
                // This makes memoization slightly more expensive since the first reference will incur an extra
                // lookup (for the put).
                // Note that recursive producers will trigger an infinite loop.
                final Optional<?> candidate = context.get(identifier);
                if (null == candidate) {
                    result = supplier.apply(divolteEvent, context);
                    context.put(identifier, result);
                } else {
                    result = (Optional<T>) candidate;
                }
            } else {
                result = supplier.apply(divolteEvent, context);
            }
            return result;
        }

        public BooleanValueProducer equalTo(final ValueProducer<T> other) {
            return new BooleanValueProducer(
                    identifier + ".equalTo(" + other.identifier + ")",
                    (e,c) -> Optional.of(this.produce(e, c).equals(other.produce(e, c))));
        }

        public BooleanValueProducer equalTo(final T literal) {
            return new BooleanValueProducer(
                    identifier + ".equalTo(" + literal + ")",
                    (e,c) -> {
                        final Optional<T> value = produce(e, c);
                        return Optional.of(value.isPresent() && value.get().equals(literal));
                    });
        }

        public BooleanValueProducer isPresent() {
            return new BooleanValueProducer(
                    identifier + ".isPresent()",
                    (e,c) -> Optional.of(produce(e, c).map((x) -> Boolean.TRUE).orElse(Boolean.FALSE)));
        }

        public BooleanValueProducer isAbsent() {
            return isPresent().negate();
        }

        abstract Optional<ValidationError> validateTypes(final Field target);

        Optional<Object> mapToGenericRecord(final T value, final Schema schema) {
            // Default mapping is the identity mapping.
            return Optional.of(value);
        }

        @Override
        public String toString() {
            return identifier;
        }
    }

    @ParametersAreNonnullByDefault
    public static class PrimitiveValueProducer<T> extends ValueProducer<T> {
        private final Class<T> type;

        /**
         * Construct a value producer that will produce a primitive value.
         * @param readableName  A human-readable description of this producer, used in error messages.
         * @param type          The type of value that this producer will produce.
         * @param supplier      A supplier that can be used to calculate the value on demand.
         * @param memoize       Whether the value should be calculated once and remembered, or on every request.
         *                      This should only be set to true when calculating the value is expensive.
         */
        PrimitiveValueProducer(final String readableName,
                               final Class<T> type,
                               final FieldSupplier<T> supplier,
                               final boolean memoize) {
            super(readableName, supplier, memoize);
            this.type = Objects.requireNonNull(type);
        }

        PrimitiveValueProducer(final String readableName,
                               final Class<T> type,
                               final FieldSupplier<T> supplier) {
            this(readableName, type, supplier, false);
        }

        @Override
        Optional<ValidationError> validateTypes(final Field target) {
            return validateTrivialUnion(target.schema(),
                                        s -> COMPATIBLE_PRIMITIVES.get(type) == s.getType(),
                                        "type must be compatible with %s", type);
        }
    }

    @ParametersAreNonnullByDefault
    public static class PrimitiveListValueProducer<T> extends ValueProducer<List<T>> {
        private final Class<T> type;

        PrimitiveListValueProducer(final String identifier,
                                   final Class<T> type,
                                   final FieldSupplier<List<T>> supplier) {
            super(identifier, supplier);
            this.type = Objects.requireNonNull(type);
        }

        @Override
        Optional<ValidationError> validateTypes(final Field target) {
            final Schema targetSchema = target.schema();
            final Optional<ValidationError> validationError =
                    validateTrivialUnion(targetSchema,
                                         s -> s.getType() == Type.ARRAY,
                                         "must map to an Avro array, not %s", target);
            return validationError.isPresent()
                    ? validationError
                    : validateTrivialUnion(unpackNullableUnion(targetSchema).get().getElementType(),
                                           s -> COMPATIBLE_PRIMITIVES.get(type) == s.getType(),
                                           "array type must be compatible with %s", type);
        }
    }

    public static class BooleanValueProducer extends PrimitiveValueProducer<Boolean> {
        BooleanValueProducer(final String identifier,
                             final FieldSupplier<Boolean> supplier) {
            super(identifier, Boolean.class, supplier);
        }

        public BooleanValueProducer or(final ValueProducer<Boolean> other) {
            return new BooleanValueProducer(
                identifier + ".or(" + other.identifier + ")",
                (e,c) -> {
                    final Optional<Boolean> left = produce(e, c);
                    final Optional<Boolean> right = other.produce(e, c);
                    return left.isPresent() && right.isPresent()
                           ? Optional.of(left.get() || right.get())
                           : Optional.empty();
                });
        }

        public BooleanValueProducer and(final ValueProducer<Boolean> other) {
            return new BooleanValueProducer(
                identifier + ".and(" + other.identifier + ")",
                (e,c) -> {
                    final Optional<Boolean> left = produce(e, c);
                    final Optional<Boolean> right = other.produce(e, c);
                    return left.isPresent() && right.isPresent()
                           ? Optional.of(left.get() && right.get())
                           : Optional.empty();
                });
        }

        public BooleanValueProducer negate() {
            return new BooleanValueProducer(
                "not(" + identifier + ")",
                (e,c) -> produce(e,c).map((b) -> !b));
            // This would have been a fine candidate use for a method reference to BooleanUtils
        }
    }

    public static class StringValueProducer extends PrimitiveValueProducer<String> {
        StringValueProducer(final String identifier,
                            final FieldSupplier<String> supplier) {
            super(identifier, String.class, supplier);
        }

        public StringValueProducer concat(final ValueProducer<Object> other) {
            return new StringValueProducer(
                identifier + ".concat(" + other.identifier + ")",
                (e,c) -> {
                    Optional<String> left = produce(e,c);
                    Optional<Object> right = other.produce(e,c);
                    if(left.isPresent() && right.isPresent()) {
                        Object r = right.get();
                        final String strRight;
                        if(r instanceof TextNode) {
                            strRight = ((TextNode)r).asText();
                        } else {
                            strRight = r.toString();
                        }
                        return Optional.of(left.get().concat(strRight));
                    } else if(left.isPresent()) {
                        return left;
                    }
                    else {
                        return right.map(Object::toString);
                    }
                }
            );
        }

        public StringValueProducer sha3_256(){
            return new StringValueProducer(identifier + ".sha3_256()",
               (e,c) ->
                   produce(e, c).map(
                       str -> {
                           final DigestSHA3 sha3 = new Digest256();
                           sha3.update(str.getBytes(Charset.forName("UTF-8")));
                           return Hex.encodeHexString(sha3.digest());
                       }
                   )
               );
        }
    }

    @SafeVarargs
    public static ValueProducer<String> concat(final ValueProducer<String>... strings) {
        final String identifier = Arrays
            .stream(strings)
            .map(p -> p.identifier)
            .collect(Collectors.joining(",", "concat(", ")"));
        return new StringValueProducer(identifier, (e,c) ->
            Arrays
                .stream(strings)
                .map(p -> p.produce(e, c))
                .reduce(
                    Optional.empty(),
                    (left, right) -> {
                        if(left.isPresent() && right.isPresent()) {
                            return Optional.of(left.get().concat(right.get()));
                        } else if(left.isPresent()) {
                            return left;
                        }
                        else {
                            return right;
                        }
                    })
        );
    }

    @SafeVarargs
    public static ValueProducer<String> concat_ws(final String separator, final ValueProducer<String>... strings) {
        final String identifier = Arrays
            .stream(strings)
            .map(p -> p.identifier)
            .collect(Collectors.joining(",", "concat_ws(" + separator + ",", ")"));
        return new StringValueProducer(identifier, (e,c) ->
            Arrays
                .stream(strings)
                .map(p -> p.produce(e, c))
                .reduce(
                    Optional.empty(),
                    (left, right) -> {
                        if(left.isPresent() && right.isPresent()) {
                            return Optional.of(left.get() + separator + right.get());
                        } else if(left.isPresent()) {
                            return left;
                        }
                        else {
                            return right;
                        }
                    })
        );
    }

    private static Optional<ValidationError> validateTrivialUnion(final Schema targetSchema,
                                                                  final Function<Schema, Boolean> validator,
                                                                  final String messageIfInvalid,
                                                                  final Object... messageParameters) {
        final Optional<Schema> resolvedSchema = unpackNullableUnion(targetSchema);
        final Optional<Optional<ValidationError>> unionValidation =
                resolvedSchema.map(s -> validator.apply(s)
                        ? Optional.empty()
                        : validationError(String.format(messageIfInvalid, messageParameters), targetSchema));
        return unionValidation.orElseGet(() -> validationError("mapping to non-trivial unions (" + targetSchema + ") is not supported", targetSchema));
    }

    private static Optional<ValidationError> validationError(final String message,
                                                             final Schema schema) {
        return Optional.of(new ValidationError(message, schema));
    }

    interface MappingAction {
        enum MappingResult {
            STOP, EXIT, CONTINUE
        }
        MappingResult perform(DivolteEvent divolteEvent,
                              Map<String,Optional<?>> context,
                              GenericRecordBuilder record);
    }
}
