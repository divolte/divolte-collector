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

package io.divolte.server;

import static io.divolte.server.BaseEventHandler.*;
import static io.divolte.server.IncomingRequestProcessor.*;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.Cookie;
import io.undertow.util.Headers;

import java.net.InetSocketAddress;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;
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
import org.apache.avro.generic.GenericRecordBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

@ParametersAreNonnullByDefault
@NotThreadSafe
public final class DslRecordMapping {
    private final Schema schema;
    private final ArrayDeque<ImmutableList.Builder<MappingAction>> stack;

    private final UserAgentParserAndCache uaParser;

    public DslRecordMapping(final Schema schema, final UserAgentParserAndCache uaParser) {
        this.schema = Objects.requireNonNull(schema);
        this.uaParser = uaParser;

        stack = new ArrayDeque<>();
        stack.add(ImmutableList.<MappingAction>builder());
    }

    /*
     * Standard actions
     */
    public <T> void map(final String fieldName, final ValueProducer<T> producer) {
        final Field field = getAndValidateField(fieldName);
        stack.getLast().add((e,c,r) -> producer.produce(e, c).ifPresent((v) -> r.set(field, v)));
    }

    public <T> void map(String fieldName, T literal) {
        final Field field = getAndValidateField(fieldName);
        stack.getLast().add((e,c,r) -> r.set(field, literal));
    }

    private Field getAndValidateField(String fieldName) {
        final Field field = schema.getField(fieldName);
        if (field == null) {
            throw new SchemaMappingException("Attempt to map onto a non-existing field: %s", fieldName);
        }

        return field;
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
               actions.stream().forEach((action) -> action.perform(e,c,r));
           }
        });
    }

    List<MappingAction> actions() {
        return stack.getLast().build();
    }

    /*
     * Casting and conversion
     */
    public ValueProducer<Integer> toInt(ValueProducer<String> source) {
        return new ValueProducer<Integer>((e,c) -> source.produce(e, c).map(Ints::tryParse), "int" + source.memoizeKey);
    }

    public ValueProducer<Long> toLong(ValueProducer<String> source) {
        return new ValueProducer<Long>((e,c) -> source.produce(e, c).map(Longs::tryParse), "long" + source.memoizeKey);
    }

    public ValueProducer<Float> toFloat(ValueProducer<String> source) {
        return new ValueProducer<Float>((e,c) -> source.produce(e, c).map(Floats::tryParse), "float" + source.memoizeKey);
    }

    public ValueProducer<Double> toDouble(ValueProducer<String> source) {
        return new ValueProducer<Double>((e,c) -> source.produce(e, c).map(Doubles::tryParse), "double" + source.memoizeKey);
    }

    public ValueProducer<Boolean> toBoolean(ValueProducer<String> source) {
        return new ValueProducer<Boolean>((e,c) -> source.produce(e, c).map(Boolean::parseBoolean), "boolean" + source.memoizeKey);
    }

    public ValueProducer<String> formatTime(ValueProducer<Long> source, String pattern) {
        // This is OK, because the RecordMapper is not thread safe.
        final DateFormat df = new SimpleDateFormat(pattern);
        return new ValueProducer<String>((e,c) -> source.produce(e,c).map((ts) -> df.format(new Date(ts))), "time" + pattern + source.memoizeKey);
    }

    /*
     * Simple field mappings
     */
    public ValueProducer<String> location() {
        return new ValueProducer<String>((e, c) -> queryParam(e, LOCATION_QUERY_PARAM), "location");
    }

    public ValueProducer<String> referer() {
        return new ValueProducer<String>((e, c) -> queryParam(e, REFERER_QUERY_PARAM), "referer");
    }

    public ValueProducer<String> eventType() {
        return new ValueProducer<String>((e,c) -> queryParam(e, EVENT_TYPE_QUERY_PARAM), "eventType");
    }

    public ValueProducer<Boolean> firstInSession() {
        return new ValueProducer<Boolean>((e,c) -> Optional.ofNullable(e.getAttachment(FIRST_IN_SESSION_KEY)), "firstInSession");
    }

    public ValueProducer<Boolean> corrupt() {
        return new ValueProducer<Boolean>((e,c) -> Optional.ofNullable(e.getAttachment(CORRUPT_EVENT_KEY)), "corruptEvent");
    }

    public ValueProducer<Boolean> duplicate() {
        return new ValueProducer<Boolean>((e,c) -> Optional.ofNullable(e.getAttachment(DUPLICATE_EVENT_KEY)), "duplicateEvent");
    }

    public ValueProducer<Long> timestamp() {
        return new ValueProducer<Long>((e,c) -> Optional.of(e.getAttachment(REQUEST_START_TIME_KEY)), "requestStartTime");
    }

    public ValueProducer<String> remoteHost() {
        return new ValueProducer<String>((e,c) -> Optional.ofNullable(e.getSourceAddress()).map(InetSocketAddress::getHostString), "remoteHost");
    }

    public ValueProducer<Integer> viewportPixelWidth() {
        return new ValueProducer<Integer>((e, c) -> queryParam(e, VIEWPORT_PIXEL_WIDTH_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int), "viewportPixelWidth");
    }

    public ValueProducer<Integer> viewportPixelHeight() {
        return new ValueProducer<Integer>((e, c) -> queryParam(e, VIEWPORT_PIXEL_HEIGHT_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int), "viewportPixelHeight");
    }

    public ValueProducer<Integer> screenPixelWidth() {
        return new ValueProducer<Integer>((e, c) -> queryParam(e, SCREEN_PIXEL_WIDTH_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int), "screenPixelWidth");
    }

    public ValueProducer<Integer> screenPixelHeight() {
        return new ValueProducer<Integer>((e, c) -> queryParam(e, SCREEN_PIXEL_HEIGHT_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int), "screenPixelHeight");
    }

    public ValueProducer<Integer> devicePixelRatio() {
        return new ValueProducer<Integer>((e, c) -> queryParam(e, DEVICE_PIXEL_RATIO_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int), "devicePixelRatio");
    }

    public ValueProducer<String> partyId() {
        return new ValueProducer<String>((e,c) -> Optional.ofNullable(e.getAttachment(PARTY_COOKIE_KEY)).map((cv) -> cv.value), "partyCoookie");
    }

    public ValueProducer<String> sessionId() {
        return new ValueProducer<String>((e,c) -> Optional.ofNullable(e.getAttachment(SESSION_COOKIE_KEY)).map((cv) -> cv.value), "sessionCookie");
    }

    public ValueProducer<String> pageViewId() {
        return new ValueProducer<String>((e,c) -> Optional.ofNullable(e.getAttachment(PAGE_VIEW_ID_KEY)), "pageView");
    }

    public ValueProducer<String> eventId() {
        return new ValueProducer<String>((e,c) -> Optional.ofNullable(e.getAttachment(EVENT_ID_KEY)), "eventId");
    }

    /*
     * User agent mapping
     */
    public ValueProducer<String> userAgentString() {
        return new ValueProducer<String>((e,c) -> Optional.ofNullable(e.getRequestHeaders().getFirst(Headers.USER_AGENT)), "userAgentString");
    }

    public UserAgentValueProducer userAgent() {
        return new UserAgentValueProducer(userAgentString(), uaParser);
    }

    public final class UserAgentValueProducer extends ValueProducer<ReadableUserAgent> {
        private UserAgentValueProducer(final ValueProducer<String> source, final UserAgentParserAndCache parser) {
            super((e,c) -> source.produce(e,c).flatMap(parser::tryParse), "userAgent");
        }

        public ValueProducer<String> name() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map(ReadableUserAgent::getName), "uaName");
        }

        public ValueProducer<String> family() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map((ua) -> ua.getFamily().getName()), "uaFamily");
        }

        public ValueProducer<String> vendor() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map(ReadableUserAgent::getProducer), "uaVendor");
        }

        public ValueProducer<String> type() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map((ua) -> ua.getType().getName()), "uaTypeName");
        }

        public ValueProducer<String> version() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map((ua) -> ua.getVersionNumber().toVersionString()), "uaVersion");
        }

        public ValueProducer<String> deviceCategory() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map((ua) -> ua.getDeviceCategory().getName()), "uaDeviceCategory");
        }

        public ValueProducer<String> osFamily() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map((ua) -> ua.getOperatingSystem().getFamily().getName()), "uaFamily");
        }

        public ValueProducer<String> osVersion() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map((ua) -> ua.getOperatingSystem().getVersionNumber().toVersionString()), "uaOsVersion");
        }

        public ValueProducer<String> osVendor() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map((ua) -> ua.getOperatingSystem().getProducer()), "uaOsVendor");
        }
    }

    /*
     * Regex mapping
     */
    public ValueProducer<Matcher> matcher(ValueProducer<String> source, String regex) {
        return new MatcherValueProducer(source, regex);
    }

    public final class MatcherValueProducer extends ValueProducer<Matcher> {
        private MatcherValueProducer(final ValueProducer<String> source, final String regex) {
            super((e,c) -> {
                return source.produce(e,c).map((s) -> Pattern.compile(regex).matcher(s));
            }, source.memoizeKey + "matcher" + regex);
        }

        public ValueProducer<Boolean> matches() {
            return new ValueProducer<Boolean>((e,c) -> this.produce(e,c).map(Matcher::matches), memoizeKey + "matches");
        }

        // Note: matches() must be called on a Matcher prior to calling group
        // In case of no match, group(...) throws an exception, in case there is
        // a match, but the group doesn't capture anything, it returns null.
        public ValueProducer<String> group(final int group) {
            return new ValueProducer<>((e,c) -> this.produce(e,c).map((m) -> m.matches() ? m.group(group) : null), memoizeKey + "group" + group);
        }

        public ValueProducer<String> group(final String group) {
            return new ValueProducer<>((e,c) -> this.produce(e,c).map((m) -> m.matches() ? m.group(group) : null), memoizeKey + "group" + group);
        }
    }

    /*
     * URI parsing
     */
    public ValueProducer<URI> parseUri(final ValueProducer<String> source) {
        return new UriValueProducer(source);
    }

    public final class UriValueProducer extends ValueProducer<URI> {
        private UriValueProducer(final ValueProducer<String> source) {
            super((e,c) -> {
                return source.produce(e, c).map((location) -> {
                   try {
                        return new URI(location);
                    } catch (Exception ignored) {
                        // When we cannot parse as URI, leave the producer empty
                        return null;
                    }
                });
            }, source.memoizeKey + "URI");
        }

        public ValueProducer<String> path() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map(URI::getPath), memoizeKey + "path");
        }

        public ValueProducer<String> rawPath() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map(URI::getRawPath), memoizeKey + "rawPath");
        }

        public ValueProducer<String> scheme() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map(URI::getScheme), memoizeKey + "scheme");
        }

        public ValueProducer<String> host() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map(URI::getHost), memoizeKey + "host");
        }

        public ValueProducer<Integer> port() {
            return new ValueProducer<Integer>((e,c) -> produce(e,c).map((uri) -> uri.getPort() != -1 ? uri.getPort() : null), memoizeKey + "port");
        }

        public ValueProducer<String> decodedQueryString() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map(URI::getQuery), memoizeKey + "queryString");
        }

        public ValueProducer<String> rawQueryString() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map(URI::getRawQuery), memoizeKey + "rawQueryString");
        }

        public ValueProducer<String> decodedFragment() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map(URI::getFragment), memoizeKey + "fragment");
        }

        public ValueProducer<String> rawFragment() {
            return new ValueProducer<String>((e,c) -> produce(e,c).map(URI::getRawFragment), memoizeKey + "rawFragment");
        }

        public QueryStringValueProducer query() {
            return new QueryStringValueProducer(rawQueryString());
        }
    }

    public final class QueryStringValueProducer extends ValueProducer<Map<String,List<String>>> {
        private QueryStringValueProducer(final ValueProducer<String> source) {
            super((e,c) -> source.produce(e, c).map(QueryStringParser::parseQueryString), source.memoizeKey + "query");
        }

        public ValueProducer<String> value(final String key) {
            // Note that we do not check for the empty list, as is could not exist; if the key is in the map, there is at least one element in the list
            return new ValueProducer<String>((e,c) -> produce(e,c).map((qs) -> qs.get(key)).map((l) -> l.get(0)), memoizeKey + "value" + key);
        }

        public ValueProducer<List<String>> valueList(final String key) {
            return new ValueProducer<List<String>>((e,c) -> produce(e,c).map((qs) -> qs.get(key)), memoizeKey + "valueList" + key);
        }
    }

    /*
     * Cookie mapping
     */
    public ValueProducer<String> cookie(String name) {
        return new ValueProducer<String>((e,c) -> Optional.ofNullable(e.getRequestCookies().get(name)).map(Cookie::getValue), "cookie" + name);
    }

    /*
     * Custom event parameter mapping
     */
    public ValueProducer<String> eventParameter(String name) {
        return new ValueProducer<String>((e, c) -> queryParam(e, EVENT_TYPE_QUERY_PARAM + "." + name), "eventParam" + name);
    }

    /*
     * Internal methods and types
     */
    private static Optional<String> queryParam(final HttpServerExchange exchange, final String param) {
        return Optional.ofNullable(exchange.getQueryParameters().get(param)).map(Deque::getFirst);
    }

    private static class ValueProducer<T> {
        private final BiFunction<HttpServerExchange, Map<String,Object>, Optional<T>> supplier;
        private final boolean memoize;
        protected final String memoizeKey;

        public ValueProducer(final BiFunction<HttpServerExchange, Map<String,Object>, Optional<T>> supplier, final String memoizeKey, boolean memoize) {
            this.supplier = supplier;
            this.memoizeKey = memoizeKey;
            this.memoize = memoize;
        }

        public ValueProducer(final BiFunction<HttpServerExchange, Map<String,Object>, Optional<T>> supplier, final String memoizeKey) {
            this(supplier, memoizeKey, false);
        }

        @SuppressWarnings("unchecked")
        public Optional<T> produce(final HttpServerExchange exchange, final Map<String,Object> context) {
            if (memoize) {
                return (Optional<T>) context.computeIfAbsent(memoizeKey, (ignored) -> supplier.apply(exchange, context));
            } else {
                return supplier.apply(exchange, context);
            }
        }

        public ValueProducer<Boolean> equalTo(final ValueProducer<T> other) {
            return new ValueProducer<Boolean>((e, c) -> {
                Optional<T> left = this.produce(e, c);
                Optional<T> right = other.produce(e, c);
                return left.isPresent() && right.isPresent() ? Optional.of(left.get().equals(right.get())) : Optional.of(false);
            }, "equals" + this.memoizeKey + other.memoizeKey, this.memoize || other.memoize);
        }

        public ValueProducer<Boolean> equalTo(final T other) {
            return new ValueProducer<Boolean>((e, c) -> {
                Optional<T> left = this.produce(e, c);
                return left.isPresent() ? Optional.of(left.get().equals(other)) : Optional.of(false);
            }, other.toString() + this.memoizeKey, this.memoize);
        }
    }

    static interface MappingAction {
        void perform(HttpServerExchange echange, Map<String,Object> context, GenericRecordBuilder record);
    }
}
