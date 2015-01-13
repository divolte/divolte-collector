/*
 * Copyright 2014 GoDataDriven B.V.
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

import static io.divolte.server.BaseEventHandler.*;
import static io.divolte.server.IncomingRequestProcessor.*;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.ip2geo.LookupService.ClosedServiceException;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.Cookie;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

import net.sf.uadetector.ReadableUserAgent;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.Subdivision;
import com.maxmind.geoip2.record.Traits;

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
        if (!producer.validateTypes(field)) {
            throw new SchemaMappingException("Type mismatch. Cannot map the result of %s onto a field of type %s (type of value and schema of field do not match).", producer.identifier, field.schema());
        }
        stack.getLast().add((e,c,r) -> {
                producer.produce(e, c).ifPresent((v) -> r.set(field, v));
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
        stack.add(ImmutableList.<MappingAction>builder());
        closure.run();

        final List<MappingAction> actions = stack.removeLast().build();
        stack.getLast().add((e,c,r) -> {
           if (condition.produce(e,c).orElse(false)) {
               for (MappingAction action : actions) {
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
        stack.add(ImmutableList.<MappingAction>builder());
        closure.run();

        final List<MappingAction> actions = stack.removeLast().build();
        stack.getLast().add((e,c,r) -> {
           for (MappingAction action : actions) {
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
                (e,c,r) -> condition.produce(e,c)
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

    public ValueProducer<Boolean> toBoolean(final ValueProducer<String> source) {
        return new BooleanValueProducer(
                "parse(" + source.identifier + " to bool)",
                (e,c) -> source.produce(e, c).map(Boolean::parseBoolean));
    }

    /*
     * Simple field mappings
     */
    public ValueProducer<String> location() {
        return new PrimitiveValueProducer<>("location()", String.class, (e, c) -> e.getAttachment(LOCATION_KEY));
    }

    public ValueProducer<String> referer() {
        return new PrimitiveValueProducer<>("referer()", String.class, (e, c) -> e.getAttachment(REFERER_KEY));
    }

    public ValueProducer<String> eventType() {
        return new PrimitiveValueProducer<>("eventType()", String.class, (e,c) -> e.getAttachment(EVENT_TYPE_KEY));
    }

    public ValueProducer<Boolean> firstInSession() {
        return new BooleanValueProducer("firstInSession()", (e,c) -> Optional.ofNullable(e.getAttachment(FIRST_IN_SESSION_KEY)));
    }

    public ValueProducer<Boolean> corrupt() {
        return new BooleanValueProducer("corrupt()", (e,c) -> Optional.ofNullable(e.getAttachment(CORRUPT_EVENT_KEY)));
    }

    public ValueProducer<Boolean> duplicate() {
        return new BooleanValueProducer("duplicate()", (e,c) -> Optional.ofNullable(e.getAttachment(DUPLICATE_EVENT_KEY)));
    }

    public ValueProducer<Long> timestamp() {
        return new PrimitiveValueProducer<>("timestamp()", Long.class, (e,c) -> Optional.of(e.getAttachment(REQUEST_START_TIME_KEY)));
    }

    public ValueProducer<String> remoteHost() {
        return new PrimitiveValueProducer<>("remoteHost()", String.class, (e,c) -> Optional.ofNullable(e.getSourceAddress()).map(InetSocketAddress::getHostString));
    }

    public ValueProducer<Integer> viewportPixelWidth() {
        return new PrimitiveValueProducer<>("viewportPixelWidth()", Integer.class, (e, c) -> e.getAttachment(VIEWPORT_PIXEL_WIDTH_KEY));
    }

    public ValueProducer<Integer> viewportPixelHeight() {
        return new PrimitiveValueProducer<>("viewportPixelHeight()", Integer.class, (e, c) -> e.getAttachment(VIEWPORT_PIXEL_HEIGHT_KEY));
    }

    public ValueProducer<Integer> screenPixelWidth() {
        return new PrimitiveValueProducer<>("screenPixelWidth()", Integer.class, (e, c) -> e.getAttachment(SCREEN_PIXEL_WIDTH_KEY));
    }

    public ValueProducer<Integer> screenPixelHeight() {
        return new PrimitiveValueProducer<>("screenPixelHeight()", Integer.class, (e, c) -> e.getAttachment(SCREEN_PIXEL_HEIGHT_KEY));
    }

    public ValueProducer<Integer> devicePixelRatio() {
        return new PrimitiveValueProducer<>("devicePixelRatio()", Integer.class, (e, c) -> e.getAttachment(DEVICE_PIXEL_RATIO_KEY));
    }

    public ValueProducer<String> partyId() {
        return new PrimitiveValueProducer<>("partyId()", String.class, (e,c) -> Optional.ofNullable(e.getAttachment(PARTY_COOKIE_KEY)).map((cv) -> cv.value));
    }

    public ValueProducer<String> sessionId() {
        return new PrimitiveValueProducer<>("sessionId()", String.class, (e,c) -> Optional.ofNullable(e.getAttachment(SESSION_COOKIE_KEY)).map((cv) -> cv.value));
    }

    public ValueProducer<String> pageViewId() {
        return new PrimitiveValueProducer<>("pageViewId()", String.class, (e,c) -> Optional.ofNullable(e.getAttachment(PAGE_VIEW_ID_KEY)));
    }

    public ValueProducer<String> eventId() {
        return new PrimitiveValueProducer<>("eventId()", String.class, (e,c) -> Optional.ofNullable(e.getAttachment(EVENT_ID_KEY)));
    }

    /*
     * User agent mapping
     */
    public ValueProducer<String> userAgentString() {
        return new PrimitiveValueProducer<>("userAgentString()", String.class, (e,c) -> Optional.ofNullable(e.getRequestHeaders().getFirst(Headers.USER_AGENT)));
    }

    public UserAgentValueProducer userAgent() {
        return new UserAgentValueProducer(userAgentString(), uaParser);
    }

    public final class UserAgentValueProducer extends ValueProducer<ReadableUserAgent> {
        private UserAgentValueProducer(final ValueProducer<String> source, final UserAgentParserAndCache parser) {
            super("userAgent()", (e,c) -> source.produce(e,c).flatMap(parser::tryParse), true);
        }

        public ValueProducer<String> name() {
            return new PrimitiveValueProducer<>(identifier + ".name()", String.class, (e,c) -> produce(e,c).map(ReadableUserAgent::getName));
        }

        public ValueProducer<String> family() {
            return new PrimitiveValueProducer<>(identifier + ".family()", String.class, (e,c) -> produce(e,c).map((ua) -> ua.getFamily().getName()));
        }

        public ValueProducer<String> vendor() {
            return new PrimitiveValueProducer<>(identifier + ".vendor()", String.class, (e,c) -> produce(e,c).map(ReadableUserAgent::getProducer));
        }

        public ValueProducer<String> type() {
            return new PrimitiveValueProducer<>(identifier + ".type()", String.class, (e,c) -> produce(e,c).map((ua) -> ua.getType().getName()));
        }

        public ValueProducer<String> version() {
            return new PrimitiveValueProducer<>(identifier + ".version()", String.class, (e,c) -> produce(e,c).map((ua) -> ua.getVersionNumber().toVersionString()));
        }

        public ValueProducer<String> deviceCategory() {
            return new PrimitiveValueProducer<>(identifier + ".deviceCategory()", String.class, (e,c) -> produce(e,c).map((ua) -> ua.getDeviceCategory().getName()));
        }

        public ValueProducer<String> osFamily() {
            return new PrimitiveValueProducer<>(identifier + ".osFamily()", String.class, (e,c) -> produce(e,c).map((ua) -> ua.getOperatingSystem().getFamily().getName()));
        }

        public ValueProducer<String> osVersion() {
            return new PrimitiveValueProducer<>(identifier + ".osVersion()", String.class, (e,c) -> produce(e,c).map((ua) -> ua.getOperatingSystem().getVersionNumber().toVersionString()));
        }

        public ValueProducer<String> osVendor() {
            return new PrimitiveValueProducer<>(identifier + ".osVendor()", String.class, (e,c) -> produce(e,c).map((ua) -> ua.getOperatingSystem().getProducer()));
        }

        @Override
        boolean validateTypes(Field target) {
            // this field cannot be directly mapped onto a Avro schema
            return false;
        }
    }

    /*
     * Regex mapping
     */
    public ValueProducer<Matcher> matcher(final ValueProducer<String> source, final String regex) {
        return new MatcherValueProducer(source, regex);
    }

    public final static class MatcherValueProducer extends ValueProducer<Matcher> {
        private MatcherValueProducer(final ValueProducer<String> source, final String regex) {
            super(
                    "match(" + regex + " against " + source.identifier + ")",
                    (e,c) -> source.produce(e,c).map((s) -> Pattern.compile(regex).matcher(s)),
                    true);
        }

        public ValueProducer<Boolean> matches() {
            return new BooleanValueProducer(
                    identifier + ".matches()",
                    (e,c) -> produce(e,c).map(Matcher::matches));
        }

        // Note: matches() must be called on a Matcher prior to calling group
        // In case of no match, group(...) throws an exception, in case there is
        // a match, but the group doesn't capture anything, it returns null.
        public ValueProducer<String> group(final int group) {
            return new PrimitiveValueProducer<>(
                    identifier + ".group(" + group + ")",
                    String.class,
                    (e,c) -> produce(e,c).map((m) -> m.matches() ? m.group(group) : null));
        }

        public ValueProducer<String> group(final String group) {
            return new PrimitiveValueProducer<>(
                    identifier + ".group(" + group + ")",
                    String.class,
                    (e,c) -> produce(e,c).map((m) -> m.matches() ? m.group(group) : null));
        }

        @Override
        boolean validateTypes(Field target) {
            // this field cannot be directly mapped onto a Avro schema
            return false;
        }
    }

    /*
     * URI parsing
     */
    public ValueProducer<URI> parseUri(final ValueProducer<String> source) {
        return new UriValueProducer(source);
    }

    public final static class UriValueProducer extends ValueProducer<URI> {
        private UriValueProducer(final ValueProducer<String> source) {
            super(
                    "parse(" + source.identifier + " to uri)",
                    (e,c) -> source.produce(e, c).map((location) -> {
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
            return new PrimitiveValueProducer<>(
                    identifier + ".path()",
                    String.class,
                    (e,c) -> produce(e,c).map(URI::getPath));
        }

        public ValueProducer<String> rawPath() {
            return new PrimitiveValueProducer<>(
                    identifier + ".rawPath()",
                    String.class,
                    (e,c) -> produce(e,c).map(URI::getRawPath));
        }

        public ValueProducer<String> scheme() {
            return new PrimitiveValueProducer<>(
                    identifier + ".scheme()",
                    String.class,
                    (e,c) -> produce(e,c).map(URI::getScheme));
        }

        public ValueProducer<String> host() {
            return new PrimitiveValueProducer<>(
                    identifier + ".host()",
                    String.class,
                    (e,c) -> produce(e,c).map(URI::getHost));
        }

        public ValueProducer<Integer> port() {
            return new PrimitiveValueProducer<>(
                    identifier + ".port()",
                    Integer.class,
                    (e,c) -> produce(e,c).map((uri) -> uri.getPort() != -1 ? uri.getPort() : null));
        }

        public ValueProducer<String> decodedQueryString() {
            return new PrimitiveValueProducer<>(
                    identifier + ".decodedQueryString()",
                    String.class,
                    (e,c) -> produce(e,c).map(URI::getQuery));
        }

        public ValueProducer<String> rawQueryString() {
            return new PrimitiveValueProducer<>(
                    identifier + ".rawQueryString()",
                    String.class,
                    (e,c) -> produce(e,c).map(URI::getRawQuery));
        }

        public ValueProducer<String> decodedFragment() {
            return new PrimitiveValueProducer<>(
                    identifier + ".decodedFragment()",
                    String.class,
                    (e,c) -> produce(e,c).map(URI::getFragment));
        }

        public ValueProducer<String> rawFragment() {
            return new PrimitiveValueProducer<>(
                    identifier + ".rawFragment()",
                    String.class,
                    (e,c) -> produce(e,c).map(URI::getRawFragment));
        }

        public QueryStringValueProducer query() {
            return new QueryStringValueProducer(rawQueryString());
        }

        @Override
        boolean validateTypes(Field target) {
            // this field cannot be directly mapped onto a Avro schema
            return false;
        }
    }

    public final static class QueryStringValueProducer extends ValueProducer<Map<String,List<String>>> {
        private QueryStringValueProducer(final ValueProducer<String> source) {
            super(
                    "parse (" + source.identifier + " to querystring)",
                    (e,c) -> source.produce(e, c).map(QueryStringParser::parseQueryString),
                    true);
        }

        public ValueProducer<String> value(final String key) {
            // Note that we do not check for the empty list, as is could not exist; if the key is in the map, there is at least one element in the list
            return new PrimitiveValueProducer<>(
                    identifier + ".value(" + key + ")",
                    String.class,
                    (e,c) -> produce(e,c).map((qs) -> qs.get(key)).map((l) -> l.get(0)));
        }

        public ValueProducer<List<String>> valueList(final String key) {
            return new PrimitiveListValueProducer<>(
                    identifier + ".valueList(" + key + ")",
                    String.class,
                    (e,c) -> produce(e,c).map((qs) -> qs.get(key)));
        }

        @Override
        boolean validateTypes(Field target) {
            Optional<Schema> targetSchema = unpackNullableUnion(target.schema());
            return targetSchema
                .map((s) -> s.getType() == Type.MAP &&
                            s.getValueType().getType() == Type.ARRAY &&
                            s.getValueType().getElementType().getType() == Type.STRING)
                .orElse(false);
        }
    }

    /*
     * Cookie mapping
     */
    public ValueProducer<String> cookie(final String name) {
        return new PrimitiveValueProducer<>(
                "cookie(" + name + ")",
                String.class,
                (e,c) -> Optional.ofNullable(e.getRequestCookies().get(name)).map(Cookie::getValue));
    }

    /*
     * Custom event parameter mapping
     */
    public ValueProducer<String> eventParameter(final String name) {
        return new PrimitiveValueProducer<>(
                "eventParameter(" + name + ")",
                String.class,
                (e, c) -> e.getAttachment(EVENT_PARAM_PRODUCER_KEY).apply(name));
    }

    /*
     * Custom header mapping
     */
    public HeaderValueProducer header(final String name) {
        return new HeaderValueProducer(name);
    }

    public final static class HeaderValueProducer extends PrimitiveListValueProducer<String> {
        private final static Joiner COMMA_JOINER = Joiner.on(',');

        private HeaderValueProducer(final String name) {
            super(
                    "header(" + name + ")",
                    String.class,
                    (e,c) -> Optional.ofNullable(e.getRequestHeaders().get(name)));
        }

        public ValueProducer<String> first() {
            return new PrimitiveValueProducer<>(
                    identifier + ".first()",
                    String.class,
                    (e,c) -> produce(e, c).map((hv) -> ((HeaderValues) hv).getFirst()));
        }

        public ValueProducer<String> last() {
            return new PrimitiveValueProducer<>(
                    identifier + ".last()",
                    String.class,
                    (e,c) -> produce(e, c).map((hv) -> ((HeaderValues) hv).getLast()));
        }

        public ValueProducer<String> commaSeparated() {
            return new PrimitiveValueProducer<String>(
                    identifier + ".commaSeparated()",
                    String.class,
                    (e,c) -> produce(e, c).map(COMMA_JOINER::join));
        }
    }

    /*
     * IP to geo mapping
     */
    public GeoIpValueProducer ip2geo(final ValueProducer<String> source) {
        return new GeoIpValueProducer(
                new PrimitiveValueProducer<>(
                        "parse(" + source.identifier + " to IP address)",
                        InetAddress.class,
                        (e,c) -> source.produce(e, c).flatMap(DslRecordMapping::tryParseIpv4),
                        true),
                verifyAndReturnLookupService());
    }

    public GeoIpValueProducer ip2geo() {
        return new GeoIpValueProducer(
                new PrimitiveValueProducer<>(
                        "<remote host IP>",
                        InetAddress.class,
                        (e,c) -> Optional.ofNullable(e.getSourceAddress()).map(InetSocketAddress::getAddress)),
                verifyAndReturnLookupService());
    }

    private LookupService verifyAndReturnLookupService() {
        return geoIpService.orElseThrow(() -> new SchemaMappingException("Attempt to use a ip2geo mapping, while the ip2geo lookup database is not configured."));
    }

    public final static class GeoIpValueProducer extends ValueProducer<CityResponse> {
        private GeoIpValueProducer(final ValueProducer<InetAddress> source, final LookupService service) {
            super(
                    "ip2geo(" + source.identifier + ")",
                    (e,c) -> source.produce(e, c).flatMap((address) -> {
                        try {
                            return service.lookup(address);
                        } catch (ClosedServiceException ex) {
                            return null;
                        }
                    }),
                    true);
        }

        public ValueProducer<Integer> cityId() {
            return new PrimitiveValueProducer<>(
                    identifier + ".cityId()",
                    Integer.class,
                    (e,c) -> produce(e, c).map((r) -> r.getCity()).map(City::getGeoNameId));
        }

        public ValueProducer<String> cityName() {
            return new PrimitiveValueProducer<>(
                    identifier + ".cityName()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getCity()).map(City::getName));
        }

        public ValueProducer<String> continentCode() {
            return new PrimitiveValueProducer<>(
                    identifier + ".continentCode()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getContinent()).map(Continent::getCode));
        }

        public ValueProducer<Integer> continentId() {
            return new PrimitiveValueProducer<>(
                    identifier + ".continentId()",
                    Integer.class,
                    (e,c) -> produce(e, c).map((r) -> r.getContinent()).map(Continent::getGeoNameId));
        }

        public ValueProducer<String> continentName() {
            return new PrimitiveValueProducer<>(
                    identifier + ".continentName()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getContinent()).map(Continent::getName));
        }

        public ValueProducer<String> countryCode() {
            return new PrimitiveValueProducer<>(
                    identifier + ".countryCode()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getCountry()).map(Country::getIsoCode));
        }

        public ValueProducer<Integer> countryId() {
            return new PrimitiveValueProducer<>(
                    identifier + ".countryId()",
                    Integer.class,
                    (e,c) -> produce(e, c).map((r) -> r.getCountry()).map(Country::getGeoNameId));
        }

        public ValueProducer<String> countryName() {
            return new PrimitiveValueProducer<>(
                    identifier + ".countryName()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getCountry()).map(Country::getName));
        }

        public ValueProducer<Double> latitude() {
            return new PrimitiveValueProducer<>(
                    identifier + ".latitude()",
                    Double.class,
                    (e,c) -> produce(e, c).map((r) -> r.getLocation()).map(Location::getLatitude));
        }

        public ValueProducer<Double> longitude() {
            return new PrimitiveValueProducer<>(
                    identifier + ".latitude()",
                    Double.class,
                    (e,c) -> produce(e, c).map((r) -> r.getLocation()).map(Location::getLongitude));
        }

        public ValueProducer<Integer> metroCode() {
            return new PrimitiveValueProducer<>(
                    identifier + ".metroCode()",
                    Integer.class,
                    (e,c) -> produce(e, c).map((r) -> r.getLocation()).map(Location::getMetroCode));
        }

        public ValueProducer<String> timeZone() {
            return new PrimitiveValueProducer<>(
                    identifier + ".timeZone()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getLocation()).map(Location::getTimeZone));
        }

        public ValueProducer<String> mostSpecificSubdivisionCode() {
            return new PrimitiveValueProducer<>(
                    identifier + ".mostSpecificSubdivisionCode()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getMostSpecificSubdivision()).map(Subdivision::getIsoCode));
        }

        public ValueProducer<Integer> mostSpecificSubdivisionId() {
            return new PrimitiveValueProducer<>(
                    identifier + ".mostSpecificSubdivisionId()",
                    Integer.class,
                    (e,c) -> produce(e, c).map((r) -> r.getMostSpecificSubdivision()).map(Subdivision::getGeoNameId));
        }

        public ValueProducer<String> mostSpecificSubdivisionName() {
            return new PrimitiveValueProducer<>(
                    identifier + ".mostSpecificSubdivisionName()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getMostSpecificSubdivision()).map(Subdivision::getName));
        }

        public ValueProducer<String> postalCode() {
            return new PrimitiveValueProducer<>(
                    identifier + ".postalCode()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getPostal()).map(Postal::getCode));
        }

        public ValueProducer<String> registeredCountryCode() {
            return new PrimitiveValueProducer<>(
                    identifier + ".registeredCountryCode()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getRegisteredCountry()).map(Country::getIsoCode));
        }

        public ValueProducer<Integer> registeredCountryId() {
            return new PrimitiveValueProducer<>(
                    identifier + ".registeredCountryId()",
                    Integer.class,
                    (e,c) -> produce(e, c).map((r) -> r.getRegisteredCountry()).map(Country::getGeoNameId));
        }

        public ValueProducer<String> registeredCountryName() {
            return new PrimitiveValueProducer<>(
                    identifier + ".registeredCountryName()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getRegisteredCountry()).map(Country::getName));
        }

        public ValueProducer<String> representedCountryCode() {
            return new PrimitiveValueProducer<>(
                    identifier + ".representedCountryCode()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getRepresentedCountry()).map(Country::getIsoCode));
        }

        public ValueProducer<Integer> representedCountryId() {
            return new PrimitiveValueProducer<>(
                    identifier + ".representedCountryId()",
                    Integer.class,
                    (e,c) -> produce(e, c).map((r) -> r.getRepresentedCountry()).map(Country::getGeoNameId));
        }

        public ValueProducer<String> representedCountryName() {
            return new PrimitiveValueProducer<>(
                    identifier + ".representedCountryName()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getRepresentedCountry()).map(Country::getName));
        }

        public ValueProducer<List<String>> subdivisionCodes() {
            return new PrimitiveListValueProducer<>(
                    identifier + ".subdivisionCodes()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getSubdivisions()).map((l) -> Lists.transform(l, Subdivision::getIsoCode)));
        }

        public ValueProducer<List<Integer>> subdivisionIds() {
            return new PrimitiveListValueProducer<>(
                    identifier + ".subdivisionIds()",
                    Integer.class,
                    (e,c) -> produce(e, c).map((r) -> r.getSubdivisions()).map((l) -> Lists.transform(l, Subdivision::getGeoNameId)));
        }

        public ValueProducer<List<String>> subdivisionNames() {
            return new PrimitiveListValueProducer<>(
                    identifier + ".subdivisionNames()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getSubdivisions()).map((l) -> Lists.transform(l, Subdivision::getName)));
        }

        public ValueProducer<Integer> autonomousSystemNumber() {
            return new PrimitiveValueProducer<>(
                    identifier + ".autonomousSystemNumber()",
                    Integer.class,
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::getAutonomousSystemNumber));
        }

        public ValueProducer<String> autonomousSystemOrganization() {
            return new PrimitiveValueProducer<>(
                    identifier + ".autonomousSystemOrganization()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::getAutonomousSystemOrganization));
        }

        public ValueProducer<String> domain() {
            return new PrimitiveValueProducer<>(
                    identifier + ".domain()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::getDomain));
        }

        public ValueProducer<String> isp() {
            return new PrimitiveValueProducer<>(
                    identifier + ".isp()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::getIsp));
        }

        public ValueProducer<String> organisation() {
            return new PrimitiveValueProducer<>(
                    identifier + ".organisation()",
                    String.class,
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::getOrganization));
        }

        public ValueProducer<Boolean> anonymousProxy() {
            return new BooleanValueProducer(
                    identifier + ".anonymousProxy()",
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::isAnonymousProxy));
        }

        public ValueProducer<Boolean> satelliteProvider() {
            return new BooleanValueProducer(
                    identifier + ".satelliteProvider()",
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::isSatelliteProvider));
        }

        @Override
        boolean validateTypes(Field target) {
            return false;
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

    private static abstract class ValueProducer<T> {
        private final BiFunction<HttpServerExchange, Map<String,Object>, Optional<T>> supplier;
        private final boolean memoize;
        protected final String identifier;

        public ValueProducer(final String identifier, final BiFunction<HttpServerExchange, Map<String,Object>, Optional<T>> supplier, final boolean memoize) {
            this.identifier = identifier;
            this.supplier = supplier;
            this.memoize = memoize;
        }

        @SuppressWarnings("unchecked")
        public Optional<T> produce(final HttpServerExchange exchange, final Map<String,Object> context) {
            if (memoize) {
                return (Optional<T>) context.computeIfAbsent(identifier, (ignored) -> supplier.apply(exchange, context));
            } else {
                return supplier.apply(exchange, context);
            }
        }

        public ValueProducer<Boolean> equalTo(final ValueProducer<T> other) {
            return new BooleanValueProducer(
                    identifier + ".equalTo(" + other.identifier + ")",
                    (e, c) -> {
                        final Optional<T> left = this.produce(e, c);
                        final Optional<T> right = other.produce(e, c);
                        return left.isPresent() && right.isPresent() ? Optional.of(left.get().equals(right.get())) : Optional.of(false);
                    });
        }

        public ValueProducer<Boolean> equalTo(final T literal) {
            return new BooleanValueProducer(
                    identifier + ".equalTo(" + literal + ")",
                    (e, c) -> {
                        final Optional<T> value = produce(e, c);
                        return value.isPresent() ? Optional.of(value.get().equals(literal)) : Optional.of(false);
                    });
        }

        public ValueProducer<Boolean> isPresent() {
            return new BooleanValueProducer(
                    identifier + ".isPresent()",
                    (e,c) -> Optional.of(produce(e, c).map((x) -> Boolean.TRUE).orElse(Boolean.FALSE)));
        }

        public ValueProducer<Boolean> isAbsent() {
            return new BooleanValueProducer(
                    identifier + ".isAbsent()",
                    (e,c) -> Optional.of(produce(e, c).map((x) -> Boolean.FALSE).orElse(Boolean.TRUE)));
        }

        abstract boolean validateTypes(final Field target);

        @Override
        public String toString() {
            return identifier;
        }
    }

    @ParametersAreNonnullByDefault
    private static class PrimitiveValueProducer<T> extends ValueProducer<T> {
        private final Class<T> type;

        public PrimitiveValueProducer(final String identifier, final Class<T> type, final BiFunction<HttpServerExchange, Map<String,Object>, Optional<T>> supplier, final boolean memoize) {
            super(identifier, supplier, memoize);
            this.type = type;
        }

        public PrimitiveValueProducer(final String readableName, final Class<T> type, final BiFunction<HttpServerExchange, Map<String,Object>, Optional<T>> supplier) {
            this(readableName, type, supplier, false);
        }

        public boolean validateTypes(final Field target) {
            final Optional<Schema> targetSchema = unpackNullableUnion(target.schema());
            return targetSchema
                    .map((s) -> COMPATIBLE_PRIMITIVES.get(type) == s.getType())
                    .orElse(false);
        }
    }

    private static class PrimitiveListValueProducer<T> extends ValueProducer<List<T>> {
        private final Class<T> type;

        public PrimitiveListValueProducer(String identifier, Class<T> type, BiFunction<HttpServerExchange, Map<String, Object>, Optional<List<T>>> supplier) {
            this(identifier, type, supplier, false);
        }

        public PrimitiveListValueProducer(String identifier, Class<T> type, BiFunction<HttpServerExchange, Map<String, Object>, Optional<List<T>>> supplier, final boolean memoize) {
            super(identifier, supplier, memoize);
            this.type = type;
        }

        @Override
        boolean validateTypes(final Field target) {
            final Optional<Schema> targetSchema = unpackNullableUnion(target.schema());
            return targetSchema
                    .map((s) -> s.getType() == Type.ARRAY &&
                        unpackNullableUnion(s.getElementType()).map((sub) -> COMPATIBLE_PRIMITIVES.get(type) == sub.getType())
                                                               .orElse(false))
                    .orElse(false);
        }
    }

    private static class BooleanValueProducer extends PrimitiveValueProducer<Boolean> {
        private BooleanValueProducer(String identifier, BiFunction<HttpServerExchange, Map<String, Object>, Optional<Boolean>> supplier, boolean memoize) {
            super(identifier, Boolean.class, supplier, memoize);
        }

        private BooleanValueProducer(String identifier, BiFunction<HttpServerExchange, Map<String, Object>, Optional<Boolean>> supplier) {
            super(identifier, Boolean.class, supplier);
        }

        @SuppressWarnings("unused")
        public ValueProducer<Boolean> or(final ValueProducer<Boolean> other) {
            return new BooleanValueProducer(
                    identifier + ".or(" + other.identifier + ")",
                    (e,c) -> {
                        final Optional<Boolean> left = produce(e,c);
                        final Optional<Boolean> right = other.produce(e,c);
                        return left.isPresent() && right.isPresent() ? Optional.of(left.get() || right.get()) : Optional.empty();
                    });
        }

        @SuppressWarnings("unused")
        public ValueProducer<Boolean> and(final ValueProducer<Boolean> other) {
            return new BooleanValueProducer(
                    identifier + ".and(" + other.identifier + ")",
                    (e,c) -> {
                        final Optional<Boolean> left = produce(e,c);
                        final Optional<Boolean> right = other.produce(e,c);
                        return left.isPresent() && right.isPresent() ? Optional.of(left.get() && right.get()) : Optional.empty();
                    });
        }

        @SuppressWarnings("unused")
        public ValueProducer<Boolean> negate() {
            return new BooleanValueProducer(
                        "not(" + identifier + ")",
                        (e,c) -> produce(e,c).map((b) -> !b));
            // This would have been a fine candidate use for a method reference to BooleanUtils
        }
    }

    static interface MappingAction {
        enum MappingResult {
            STOP, EXIT, CONTINUE
        }
        MappingResult perform(final HttpServerExchange echange, final Map<String,Object> context, GenericRecordBuilder record);
    }
}
