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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import io.divolte.server.DivolteEvent;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.ip2geo.LookupService.ClosedServiceException;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.Cookie;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import net.sf.uadetector.ReadableUserAgent;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public DslRecordMapping(final Schema schema, final UserAgentParserAndCache uaParser, final Optional<LookupService> geoIpService) {
        this.schema = Objects.requireNonNull(schema);
        this.uaParser = Objects.requireNonNull(uaParser);
        this.geoIpService = Objects.requireNonNull(geoIpService);

        stack = new ArrayDeque<>();
        stack.add(ImmutableList.<MappingAction>builder());
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
        stack.getLast().add((h,e,c,r) -> {
            producer.produce(h,e,c)
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

        stack.getLast().add((h,e,c,r) -> {
            r.set(field, literal);
            return MappingAction.MappingResult.CONTINUE;
        });
    }

    /*
     * Higher order actions
     */
    public void when(final ValueProducer<Boolean> condition, final Runnable closure) {
        stack.add(ImmutableList.<MappingAction>builder());
        closure.run();

        final List<MappingAction> actions = stack.removeLast().build();
        stack.getLast().add((h,e,c,r) -> {
           if (condition.produce(h, e, c).orElse(false)) {
               for (MappingAction action : actions) {
                   switch (action.perform(h, e, c, r)) {
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
        stack.add(ImmutableList.<MappingAction>builder());
        closure.run();

        final List<MappingAction> actions = stack.removeLast().build();
        stack.getLast().add((h,e,c,r) -> {
           for (MappingAction action : actions) {
               switch (action.perform(h, e, c, r)) {
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
        stack.getLast().add((h,e,c,r) -> MappingAction.MappingResult.STOP);
    }

    public void exitWhen(final ValueProducer<Boolean> condition) {
        stack.getLast().add(
                (h,e,c,r) -> condition.produce(h, e, c)
                                      .map((b) -> b ? MappingAction.MappingResult.EXIT : MappingAction.MappingResult.CONTINUE)
                                      .orElse(MappingAction.MappingResult.CONTINUE));
    }

    public void exit() {
        stack.getLast().add((h,e,c,r) -> MappingAction.MappingResult.EXIT);
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
                (h,e,c) -> source.produce(h, e, c).map(Ints::tryParse));
    }

    public ValueProducer<Long> toLong(final ValueProducer<String> source) {
        return new PrimitiveValueProducer<>(
                "parse(" + source.identifier + " to int64)",
                Long.class,
                (h,e,c) -> source.produce(h, e, c).map(Longs::tryParse));
    }

    public ValueProducer<Float> toFloat(final ValueProducer<String> source) {
        return new PrimitiveValueProducer<>(
                "parse(" + source.identifier + " to fp32)",
                Float.class,
                (h,e,c) -> source.produce(h, e, c).map(Floats::tryParse));
    }

    public ValueProducer<Double> toDouble(final ValueProducer<String> source) {
        return new PrimitiveValueProducer<>(
                "parse(" + source.identifier + " to fp64)",
                Double.class,
                (h,e,c) -> source.produce(h, e, c).map(Doubles::tryParse));
    }

    public ValueProducer<Boolean> toBoolean(final ValueProducer<String> source) {
        return new BooleanValueProducer(
                "parse(" + source.identifier + " to bool)",
                (h,e,c) -> source.produce(h, e, c).map(Boolean::parseBoolean));
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
        return new PrimitiveValueProducer<>("eventType()", String.class, (h,e,c) -> e.eventType);
    }

    public ValueProducer<Boolean> firstInSession() {
        return new BooleanValueProducer("firstInSession()", (h,e,c) -> Optional.of(e.firstInSession));
    }

    public ValueProducer<Boolean> corrupt() {
        return new BooleanValueProducer("corrupt()", (h,e,c) -> Optional.of(e.corruptEvent));
    }

    public ValueProducer<Boolean> duplicate() {
        return new BooleanValueProducer("duplicate()", (h,e,c) -> Optional.ofNullable(h.getAttachment(DUPLICATE_EVENT_KEY)));
    }

    public ValueProducer<Long> timestamp() {
        return new PrimitiveValueProducer<>("timestamp()", Long.class, (h,e,c) -> Optional.of(e.requestStartTime));
    }

    public ValueProducer<Long> clientTimestamp() {
        return new PrimitiveValueProducer<>("clientTimestamp()", Long.class, (h,e,c) -> Optional.of(e.requestStartTime + e.clientUtcOffset));
    }

    public ValueProducer<String> remoteHost() {
        return new PrimitiveValueProducer<>("remoteHost()", String.class, (h,e,c) -> Optional.ofNullable(h.getSourceAddress()).map(InetSocketAddress::getHostString));
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

    public ValueProducer<String> partyId() {
        return new PrimitiveValueProducer<>("partyId()", String.class, (h,e,c) -> Optional.of(e.partyCookie.value));
    }

    public ValueProducer<String> sessionId() {
        return new PrimitiveValueProducer<>("sessionId()", String.class, (h,e,c) -> Optional.of(e.sessionCookie.value));
    }

    public ValueProducer<String> pageViewId() {
        return browserEventValueProducer("pageViewId()", String.class, e -> Optional.of(e.pageViewId));
    }

    public ValueProducer<String> eventId() {
        return new PrimitiveValueProducer<>("eventId()", String.class, (h,e,c) -> Optional.of(e.eventId));
    }

    /*
     * User agent mapping
     */
    public ValueProducer<String> userAgentString() {
        return new PrimitiveValueProducer<>("userAgentString()",
                                            String.class,
                                            (h, e, c) ->
                                                    Optional.ofNullable(h.getRequestHeaders().getFirst(Headers.USER_AGENT)));
    }

    public UserAgentValueProducer userAgent() {
        return new UserAgentValueProducer(userAgentString(), uaParser);
    }

    public final class UserAgentValueProducer extends ValueProducer<ReadableUserAgent> {
        UserAgentValueProducer(final ValueProducer<String> source, final UserAgentParserAndCache parser) {
            super("userAgent()", (h, e, c) -> source.produce(h, e, c).flatMap(parser::tryParse), true);
        }

        public ValueProducer<String> name() {
            return new PrimitiveValueProducer<>(identifier + ".name()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map(ReadableUserAgent::getName));
        }

        public ValueProducer<String> family() {
            return new PrimitiveValueProducer<>(identifier + ".family()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((ua) -> ua.getFamily().getName()));
        }

        public ValueProducer<String> vendor() {
            return new PrimitiveValueProducer<>(identifier + ".vendor()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map(ReadableUserAgent::getProducer));
        }

        public ValueProducer<String> type() {
            return new PrimitiveValueProducer<>(identifier + ".type()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((ua) -> ua.getType().getName()));
        }

        public ValueProducer<String> version() {
            return new PrimitiveValueProducer<>(identifier + ".version()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((ua) -> ua.getVersionNumber().toVersionString()));
        }

        public ValueProducer<String> deviceCategory() {
            return new PrimitiveValueProducer<>(identifier + ".deviceCategory()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((ua) -> ua.getDeviceCategory().getName()));
        }

        public ValueProducer<String> osFamily() {
            return new PrimitiveValueProducer<>(identifier + ".osFamily()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((ua) -> ua.getOperatingSystem().getFamily().getName()));
        }

        public ValueProducer<String> osVersion() {
            return new PrimitiveValueProducer<>(identifier + ".osVersion()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((ua) -> ua.getOperatingSystem().getVersionNumber().toVersionString()));
        }

        public ValueProducer<String> osVendor() {
            return new PrimitiveValueProducer<>(identifier + ".osVendor()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((ua) -> ua.getOperatingSystem().getProducer()));
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
                  (h, e, c) -> source.produce(h, e, c).map((s) -> Pattern.compile(regex).matcher(s)),
                  true);
        }

        public ValueProducer<Boolean> matches() {
            return new BooleanValueProducer(identifier + ".matches()",
                                            (h,e,c) -> produce(h, e, c).map(Matcher::matches));
        }

        // Note: matches() must be called on a Matcher prior to calling group
        // In case of no match, group(...) throws an exception, in case there is
        // a match, but the group doesn't capture anything, it returns null.
        public ValueProducer<String> group(final int group) {
            return new PrimitiveValueProducer<>(identifier + ".group(" + group + ")",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((m) -> m.matches() ? m.group(group) : null));
        }

        public ValueProducer<String> group(final String group) {
            return new PrimitiveValueProducer<>(identifier + ".group(" + group + ")",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((m) -> m.matches() ? m.group(group) : null));
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
                  (h,e,c) -> source.produce(h, e, c).map((location) -> {
                        try {
                            return new URI(location);
                        } catch (Exception ignored) {
                            // When we cannot parse as URI, leave the producer empty
                            return null;
                        }
                    }),
                  true);
        }

        public ValueProducer<String> path() {
            return new PrimitiveValueProducer<>(identifier + ".path()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map(URI::getPath));
        }

        public ValueProducer<String> rawPath() {
            return new PrimitiveValueProducer<>(identifier + ".rawPath()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map(URI::getRawPath));
        }

        public ValueProducer<String> scheme() {
            return new PrimitiveValueProducer<>(identifier + ".scheme()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map(URI::getScheme));
        }

        public ValueProducer<String> host() {
            return new PrimitiveValueProducer<>(identifier + ".host()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map(URI::getHost));
        }

        public ValueProducer<Integer> port() {
            return new PrimitiveValueProducer<>(identifier + ".port()",
                                                Integer.class,
                                                (h,e,c) -> produce(h, e, c).map((uri) -> uri.getPort() != -1 ? uri.getPort() : null));
        }

        public ValueProducer<String> decodedQueryString() {
            return new PrimitiveValueProducer<>(identifier + ".decodedQueryString()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map(URI::getQuery));
        }

        public ValueProducer<String> rawQueryString() {
            return new PrimitiveValueProducer<>(identifier + ".rawQueryString()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map(URI::getRawQuery));
        }

        public ValueProducer<String> decodedFragment() {
            return new PrimitiveValueProducer<>(identifier + ".decodedFragment()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map(URI::getFragment));
        }

        public ValueProducer<String> rawFragment() {
            return new PrimitiveValueProducer<>(identifier + ".rawFragment()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map(URI::getRawFragment));
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
        QueryStringValueProducer(final ValueProducer<String> source) {
            super("parse (" + source.identifier + " to querystring)",
                  (h,e,c) -> source.produce(h, e, c).map(QueryStringParser::parseQueryString),
                  true);
        }

        public ValueProducer<String> value(final String key) {
            // Note that we do not check for the empty list, as is could not exist; if the key is in the map, there is at least one element in the list
            return new PrimitiveValueProducer<>(identifier + ".value(" + key + ")",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((qs) -> qs.get(key)).map((l) -> l.get(0)));
        }

        public ValueProducer<List<String>> valueList(final String key) {
            return new PrimitiveListValueProducer<>(identifier + ".valueList(" + key + ")",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((qs) -> qs.get(key)));
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
    public ValueProducer<String> cookie(final String name) {
        return new PrimitiveValueProducer<>("cookie(" + name + ")",
                                            String.class,
                                            (h,e,c) -> Optional.ofNullable(h.getRequestCookies().get(name)).map(Cookie::getValue));
    }

    /*
     * Custom event parameter mapping
     */
    public final static class EventParameterValueProducer extends JsonValueProducer {
        EventParameterValueProducer() {
            super("eventParameters()", (h,e,c) -> e.eventParametersProducer.get(), true);
        }

        public ValueProducer<String> value(String name) {
            return new PrimitiveValueProducer<>(
                    identifier + ".value(" + name + ")",
                    String.class,
                    (h,e,c) -> EventParameterValueProducer.this.produce(h,e,c)
                                .map(json -> json.path(name).asText()));
        }

        public ValueProducer<JsonNode> path(final String path) {
            final JsonPath jsonPath = JsonPath.compile(path);
            return new JsonValueProducer(
                    identifier + ".path(" + path + ')',
                    (h,e,c) -> EventParameterValueProducer.this.produce(h, e, c)
                                .map(json -> jsonPath.read(json, JSON_PATH_CONFIGURATION)),
                    false);
        }
    }

    @ParametersAreNonnullByDefault
    private static class JsonValueProducer extends ValueProducer<JsonNode> {
        private static final Logger logger = LoggerFactory.getLogger(JsonValueProducer.class);

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
            return JacksonSupport.AVRO_MAPPER.checkValid(target.schema());
        }

        @Override
        Optional<Object> mapToGenericRecord(final JsonNode value,
                                            final Schema schema) throws SchemaMappingException {
            try {
                return Optional.ofNullable(JacksonSupport.AVRO_MAPPER.read(value, schema));
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
    @SuppressWarnings("unused")
    public ValueProducer<String> eventParameter(final String name) {
        final EventParameterValueProducer eventParametersProducer = eventParameters();
        return new PrimitiveValueProducer<>("eventParameter(" + name + ")",
                                            String.class,
                                            (h,e,c) -> eventParametersProducer.produce(h,e,c)
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

        HeaderValueProducer(final String name) {
            super("header(" + name + ")",
                  String.class,
                  (h,e,c) -> Optional.ofNullable(h.getRequestHeaders().get(name)));
        }

        public ValueProducer<String> first() {
            return new PrimitiveValueProducer<>(identifier + ".first()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((hv) -> ((HeaderValues) hv).getFirst()));
        }

        public ValueProducer<String> last() {
            return new PrimitiveValueProducer<>(identifier + ".last()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((hv) -> ((HeaderValues) hv).getLast()));
        }

        public ValueProducer<String> commaSeparated() {
            return new PrimitiveValueProducer<>(identifier + ".commaSeparated()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map(COMMA_JOINER::join));
        }
    }

    /*
     * IP to geo mapping
     */
    public GeoIpValueProducer ip2geo(final ValueProducer<String> source) {
        final PrimitiveValueProducer<InetAddress> addressProducer =
                new PrimitiveValueProducer<>("parse(" + source.identifier + " to IP address)",
                                             InetAddress.class,
                                             (h, e, c) -> source.produce(h, e, c).flatMap(DslRecordMapping::tryParseIpv4),
                                             true);
        return new GeoIpValueProducer(addressProducer, verifyAndReturnLookupService());
    }

    public GeoIpValueProducer ip2geo() {
        final PrimitiveValueProducer<InetAddress> addressProducer =
                new PrimitiveValueProducer<>("<remote host IP>",
                                             InetAddress.class,
                                             (h,e,c) -> Optional.ofNullable(h.getSourceAddress()).map(InetSocketAddress::getAddress));
        return new GeoIpValueProducer(addressProducer, verifyAndReturnLookupService());
    }

    private LookupService verifyAndReturnLookupService() {
        return geoIpService.orElseThrow(() -> new SchemaMappingException("Attempt to use a ip2geo mapping, while the ip2geo lookup database is not configured."));
    }

    public final static class GeoIpValueProducer extends ValueProducer<CityResponse> {
        GeoIpValueProducer(final ValueProducer<InetAddress> source, final LookupService service) {
            super("ip2geo(" + source.identifier + ")",
                  (h,e,c) -> source.produce(h, e, c).flatMap((address) -> {
                        try {
                            return service.lookup(address);
                        } catch (ClosedServiceException ex) {
                            return null;
                        }
                    }),
                  true);
        }

        public ValueProducer<Integer> cityId() {
            return new PrimitiveValueProducer<>(identifier + ".cityId()",
                                                Integer.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getCity()).map(City::getGeoNameId));
        }

        public ValueProducer<String> cityName() {
            return new PrimitiveValueProducer<>(identifier + ".cityName()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getCity()).map(City::getName));
        }

        public ValueProducer<String> continentCode() {
            return new PrimitiveValueProducer<>(identifier + ".continentCode()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getContinent()).map(Continent::getCode));
        }

        public ValueProducer<Integer> continentId() {
            return new PrimitiveValueProducer<>(identifier + ".continentId()",
                                                Integer.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getContinent()).map(Continent::getGeoNameId));
        }

        public ValueProducer<String> continentName() {
            return new PrimitiveValueProducer<>(identifier + ".continentName()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getContinent()).map(Continent::getName));
        }

        public ValueProducer<String> countryCode() {
            return new PrimitiveValueProducer<>(identifier + ".countryCode()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getCountry()).map(Country::getIsoCode));
        }

        public ValueProducer<Integer> countryId() {
            return new PrimitiveValueProducer<>(identifier + ".countryId()",
                                                Integer.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getCountry()).map(Country::getGeoNameId));
        }

        public ValueProducer<String> countryName() {
            return new PrimitiveValueProducer<>(identifier + ".countryName()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getCountry()).map(Country::getName));
        }

        public ValueProducer<Double> latitude() {
            return new PrimitiveValueProducer<>(identifier + ".latitude()",
                                                Double.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getLocation()).map(Location::getLatitude));
        }

        public ValueProducer<Double> longitude() {
            return new PrimitiveValueProducer<>(identifier + ".latitude()",
                                                Double.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getLocation()).map(Location::getLongitude));
        }

        public ValueProducer<Integer> metroCode() {
            return new PrimitiveValueProducer<>(identifier + ".metroCode()",
                                                Integer.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getLocation()).map(Location::getMetroCode));
        }

        public ValueProducer<String> timeZone() {
            return new PrimitiveValueProducer<>(identifier + ".timeZone()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getLocation()).map(Location::getTimeZone));
        }

        public ValueProducer<String> mostSpecificSubdivisionCode() {
            return new PrimitiveValueProducer<>(identifier + ".mostSpecificSubdivisionCode()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getMostSpecificSubdivision()).map(Subdivision::getIsoCode));
        }

        public ValueProducer<Integer> mostSpecificSubdivisionId() {
            return new PrimitiveValueProducer<>(identifier + ".mostSpecificSubdivisionId()",
                                                Integer.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getMostSpecificSubdivision()).map(Subdivision::getGeoNameId));
        }

        public ValueProducer<String> mostSpecificSubdivisionName() {
            return new PrimitiveValueProducer<>(identifier + ".mostSpecificSubdivisionName()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getMostSpecificSubdivision()).map(Subdivision::getName));
        }

        public ValueProducer<String> postalCode() {
            return new PrimitiveValueProducer<>(identifier + ".postalCode()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getPostal()).map(Postal::getCode));
        }

        public ValueProducer<String> registeredCountryCode() {
            return new PrimitiveValueProducer<>(identifier + ".registeredCountryCode()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getRegisteredCountry()).map(Country::getIsoCode));
        }

        public ValueProducer<Integer> registeredCountryId() {
            return new PrimitiveValueProducer<>(identifier + ".registeredCountryId()",
                                                Integer.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getRegisteredCountry()).map(Country::getGeoNameId));
        }

        public ValueProducer<String> registeredCountryName() {
            return new PrimitiveValueProducer<>(identifier + ".registeredCountryName()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getRegisteredCountry()).map(Country::getName));
        }

        public ValueProducer<String> representedCountryCode() {
            return new PrimitiveValueProducer<>(identifier + ".representedCountryCode()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getRepresentedCountry()).map(Country::getIsoCode));
        }

        public ValueProducer<Integer> representedCountryId() {
            return new PrimitiveValueProducer<>(identifier + ".representedCountryId()",
                                                Integer.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getRepresentedCountry()).map(Country::getGeoNameId));
        }

        public ValueProducer<String> representedCountryName() {
            return new PrimitiveValueProducer<>(identifier + ".representedCountryName()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getRepresentedCountry()).map(Country::getName));
        }

        public ValueProducer<List<String>> subdivisionCodes() {
            return new PrimitiveListValueProducer<>(identifier + ".subdivisionCodes()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getSubdivisions()).map((l) -> Lists.transform(l, Subdivision::getIsoCode)));
        }

        public ValueProducer<List<Integer>> subdivisionIds() {
            return new PrimitiveListValueProducer<>(identifier + ".subdivisionIds()",
                                                Integer.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getSubdivisions()).map((l) -> Lists.transform(l, Subdivision::getGeoNameId)));
        }

        public ValueProducer<List<String>> subdivisionNames() {
            return new PrimitiveListValueProducer<>(identifier + ".subdivisionNames()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getSubdivisions()).map((l) -> Lists.transform(l, Subdivision::getName)));
        }

        public ValueProducer<Integer> autonomousSystemNumber() {
            return new PrimitiveValueProducer<>(identifier + ".autonomousSystemNumber()",
                                                Integer.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getTraits()).map(Traits::getAutonomousSystemNumber));
        }

        public ValueProducer<String> autonomousSystemOrganization() {
            return new PrimitiveValueProducer<>(identifier + ".autonomousSystemOrganization()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getTraits()).map(Traits::getAutonomousSystemOrganization));
        }

        public ValueProducer<String> domain() {
            return new PrimitiveValueProducer<>(identifier + ".domain()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getTraits()).map(Traits::getDomain));
        }

        public ValueProducer<String> isp() {
            return new PrimitiveValueProducer<>(identifier + ".isp()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getTraits()).map(Traits::getIsp));
        }

        public ValueProducer<String> organisation() {
            return new PrimitiveValueProducer<>(identifier + ".organisation()",
                                                String.class,
                                                (h,e,c) -> produce(h, e, c).map((r) -> r.getTraits()).map(Traits::getOrganization));
        }

        public ValueProducer<Boolean> anonymousProxy() {
            return new BooleanValueProducer(identifier + ".anonymousProxy()",
                                            (h,e,c) -> produce(h, e, c).map((r) -> r.getTraits()).map(Traits::isAnonymousProxy));
        }

        public ValueProducer<Boolean> satelliteProvider() {
            return new BooleanValueProducer(identifier + ".satelliteProvider()",
                                            (h,e,c) -> produce(h, e, c).map((r) -> r.getTraits()).map(Traits::isSatelliteProvider));
        }

        @Override
        Optional<ValidationError> validateTypes(Field target) {
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
        } catch(NumberFormatException nfe) {
            return Optional.empty();
        }

        try {
            return Optional.of(InetAddress.getByAddress(result));
        } catch (UnknownHostException e) {
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
                                            (h,e,c) -> e.browserEventData.flatMap(fieldSupplier::apply));
    }

    @ParametersAreNonnullByDefault
    public static abstract class ValueProducer<T> {

        protected interface FieldSupplier<T> {
            Optional<T> apply(HttpServerExchange httpServerExchange,
                              DivolteEvent eventData,
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

        final Optional<T> produce(final HttpServerExchange exchange,
                                  final DivolteEvent divolteEvent,
                                  final Map<String,Optional<?>> context) {
            @SuppressWarnings("unchecked")
            final Optional<T> result = memoize
                    ? (Optional<T>)context.computeIfAbsent(identifier, (x) -> supplier.apply(exchange, divolteEvent, context))
                    : supplier.apply(exchange, divolteEvent, context);
            return result;
        }

        public ValueProducer<Boolean> equalTo(final ValueProducer<T> other) {
            return new BooleanValueProducer(
                    identifier + ".equalTo(" + other.identifier + ")",
                    (h,e,c) -> {
                        final Optional<T> left = this.produce(h, e, c);
                        final Optional<T> right = other.produce(h, e, c);
                        return left.isPresent() && right.isPresent()
                                ? Optional.of(left.get().equals(right.get()))
                                : Optional.of(false);
                    });
        }

        public ValueProducer<Boolean> equalTo(final T literal) {
            return new BooleanValueProducer(
                    identifier + ".equalTo(" + literal + ")",
                    (h,e,c) -> {
                        final Optional<T> value = produce(h, e, c);
                        return value.isPresent()
                                ? Optional.of(value.get().equals(literal))
                                : Optional.of(false);
                    });
        }

        public ValueProducer<Boolean> isPresent() {
            return new BooleanValueProducer(
                    identifier + ".isPresent()",
                    (h,e,c) -> Optional.of(produce(h, e, c).map((x) -> Boolean.TRUE).orElse(Boolean.FALSE)));
        }

        public ValueProducer<Boolean> isAbsent() {
            return new BooleanValueProducer(
                    identifier + ".isAbsent()",
                    (h,e,c) -> Optional.of(produce(h, e, c).map((x) -> Boolean.FALSE).orElse(Boolean.TRUE)));
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

        @SuppressWarnings("unused")
        public ValueProducer<Boolean> or(final ValueProducer<Boolean> other) {
            return new BooleanValueProducer(
                    identifier + ".or(" + other.identifier + ")",
                    (h,e,c) -> {
                        final Optional<Boolean> left = produce(h, e, c);
                        final Optional<Boolean> right = other.produce(h, e, c);
                        return left.isPresent() && right.isPresent()
                                ? Optional.of(left.get() || right.get())
                                : Optional.empty();
                    });
        }

        @SuppressWarnings("unused")
        public ValueProducer<Boolean> and(final ValueProducer<Boolean> other) {
            return new BooleanValueProducer(
                    identifier + ".and(" + other.identifier + ")",
                    (h,e,c) -> {
                        final Optional<Boolean> left = produce(h, e, c);
                        final Optional<Boolean> right = other.produce(h, e, c);
                        return left.isPresent() && right.isPresent()
                                ? Optional.of(left.get() && right.get())
                                : Optional.empty();
                    });
        }

        @SuppressWarnings("unused")
        public ValueProducer<Boolean> negate() {
            return new BooleanValueProducer(
                        "not(" + identifier + ")",
                        (h,e,c) -> produce(h,e,c).map((b) -> !b));
            // This would have been a fine candidate use for a method reference to BooleanUtils
        }
    }

    private static Optional<ValidationError> validateTrivialUnion(final Schema targetSchema,
                                                                  final Function<Schema, Boolean> validator,
                                                                  final String messageIfInvalid,
                                                                  Object... messageParameters) {
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
        MappingResult perform(HttpServerExchange exchange,
                              DivolteEvent divolteEvent,
                              Map<String,Optional<?>> context,
                              GenericRecordBuilder record);
    }
}
