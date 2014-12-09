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
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
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
    private final Schema schema;
    private final ArrayDeque<ImmutableList.Builder<MappingAction>> stack;

    private final UserAgentParserAndCache uaParser;
    private final Optional<LookupService> geoIpService;

    public DslRecordMapping(final Schema schema, final UserAgentParserAndCache uaParser, final Optional<LookupService> geoIpService) {
        this.schema = Objects.requireNonNull(schema);
        this.uaParser = uaParser;
        this.geoIpService = geoIpService;

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
        stack.getLast().add((e,c,r) -> producer.produce(e, c).ifPresent((v) -> r.set(field, v)));
    }

    public <T> void map(final String fieldName, final T literal) {
        final Field field = schema.getField(fieldName);
        if (field == null) {
            throw new SchemaMappingException("Field %s does not exist in Avro schema; error in mapping %s onto %s", fieldName, literal, fieldName);
        }
        stack.getLast().add((e,c,r) -> r.set(field, literal));
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
        return new ValueProducer<Integer>(
                "parse(" + source.identifier + " to int32)",
                (e,c) -> source.produce(e, c).map(Ints::tryParse));
    }

    public ValueProducer<Long> toLong(final ValueProducer<String> source) {
        return new ValueProducer<Long>(
                "parse(" + source.identifier + " to int64)",
                (e,c) -> source.produce(e, c).map(Longs::tryParse));
    }

    public ValueProducer<Float> toFloat(final ValueProducer<String> source) {
        return new ValueProducer<Float>(
                "parse(" + source.identifier + " to fp32)",
                (e,c) -> source.produce(e, c).map(Floats::tryParse));
    }

    public ValueProducer<Double> toDouble(final ValueProducer<String> source) {
        return new ValueProducer<Double>(
                "parse(" + source.identifier + " to fp64)",
                (e,c) -> source.produce(e, c).map(Doubles::tryParse));
    }

    public ValueProducer<Boolean> toBoolean(final ValueProducer<String> source) {
        return new ValueProducer<Boolean>(
                "parse(" + source.identifier + " to bool)",
                (e,c) -> source.produce(e, c).map(Boolean::parseBoolean));
    }

    /*
     * Simple field mappings
     */
    public ValueProducer<String> location() {
        return new ValueProducer<String>("location()", (e, c) -> queryParam(e, LOCATION_QUERY_PARAM));
    }

    public ValueProducer<String> referer() {
        return new ValueProducer<String>("referer()", (e, c) -> queryParam(e, REFERER_QUERY_PARAM));
    }

    public ValueProducer<String> eventType() {
        return new ValueProducer<String>("eventType()", (e,c) -> queryParam(e, EVENT_TYPE_QUERY_PARAM));
    }

    public ValueProducer<Boolean> firstInSession() {
        return new ValueProducer<Boolean>("firstInSession()", (e,c) -> Optional.ofNullable(e.getAttachment(FIRST_IN_SESSION_KEY)));
    }

    public ValueProducer<Boolean> corrupt() {
        return new ValueProducer<Boolean>("corrupt()", (e,c) -> Optional.ofNullable(e.getAttachment(CORRUPT_EVENT_KEY)));
    }

    public ValueProducer<Boolean> duplicate() {
        return new ValueProducer<Boolean>("duplicate()", (e,c) -> Optional.ofNullable(e.getAttachment(DUPLICATE_EVENT_KEY)));
    }

    public ValueProducer<Long> timestamp() {
        return new ValueProducer<Long>("timestamp()", (e,c) -> Optional.of(e.getAttachment(REQUEST_START_TIME_KEY)));
    }

    public ValueProducer<String> remoteHost() {
        return new ValueProducer<String>("remoteHost()", (e,c) -> Optional.ofNullable(e.getSourceAddress()).map(InetSocketAddress::getHostString));
    }

    public ValueProducer<Integer> viewportPixelWidth() {
        return new ValueProducer<Integer>("viewportPixelWidth()", (e, c) -> queryParam(e, VIEWPORT_PIXEL_WIDTH_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int));
    }

    public ValueProducer<Integer> viewportPixelHeight() {
        return new ValueProducer<Integer>("viewportPixelHeight()", (e, c) -> queryParam(e, VIEWPORT_PIXEL_HEIGHT_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int));
    }

    public ValueProducer<Integer> screenPixelWidth() {
        return new ValueProducer<Integer>("screenPixelWidth()", (e, c) -> queryParam(e, SCREEN_PIXEL_WIDTH_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int));
    }

    public ValueProducer<Integer> screenPixelHeight() {
        return new ValueProducer<Integer>("screenPixelHeight()", (e, c) -> queryParam(e, SCREEN_PIXEL_HEIGHT_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int));
    }

    public ValueProducer<Integer> devicePixelRatio() {
        return new ValueProducer<Integer>("devicePixelRatio()", (e, c) -> queryParam(e, DEVICE_PIXEL_RATIO_QUERY_PARAM).map(ConfigRecordMapper::tryParseBase36Int));
    }

    public ValueProducer<String> partyId() {
        return new ValueProducer<String>("partyId()", (e,c) -> Optional.ofNullable(e.getAttachment(PARTY_COOKIE_KEY)).map((cv) -> cv.value));
    }

    public ValueProducer<String> sessionId() {
        return new ValueProducer<String>("sessionId()", (e,c) -> Optional.ofNullable(e.getAttachment(SESSION_COOKIE_KEY)).map((cv) -> cv.value));
    }

    public ValueProducer<String> pageViewId() {
        return new ValueProducer<String>("pageViewId()", (e,c) -> Optional.ofNullable(e.getAttachment(PAGE_VIEW_ID_KEY)));
    }

    public ValueProducer<String> eventId() {
        return new ValueProducer<String>("eventId()", (e,c) -> Optional.ofNullable(e.getAttachment(EVENT_ID_KEY)));
    }

    /*
     * User agent mapping
     */
    public ValueProducer<String> userAgentString() {
        return new ValueProducer<String>("userAgentString()", (e,c) -> Optional.ofNullable(e.getRequestHeaders().getFirst(Headers.USER_AGENT)));
    }

    public UserAgentValueProducer userAgent() {
        return new UserAgentValueProducer(userAgentString(), uaParser);
    }

    public final class UserAgentValueProducer extends ValueProducer<ReadableUserAgent> {
        private UserAgentValueProducer(final ValueProducer<String> source, final UserAgentParserAndCache parser) {
            super("userAgent()", (e,c) -> source.produce(e,c).flatMap(parser::tryParse), true);
        }

        public ValueProducer<String> name() {
            return new ValueProducer<String>(identifier + ".name()", (e,c) -> produce(e,c).map(ReadableUserAgent::getName));
        }

        public ValueProducer<String> family() {
            return new ValueProducer<String>(identifier + ".family()", (e,c) -> produce(e,c).map((ua) -> ua.getFamily().getName()));
        }

        public ValueProducer<String> vendor() {
            return new ValueProducer<String>(identifier + ".vendor()", (e,c) -> produce(e,c).map(ReadableUserAgent::getProducer));
        }

        public ValueProducer<String> type() {
            return new ValueProducer<String>(identifier + ".type()", (e,c) -> produce(e,c).map((ua) -> ua.getType().getName()));
        }

        public ValueProducer<String> version() {
            return new ValueProducer<String>(identifier + ".version()", (e,c) -> produce(e,c).map((ua) -> ua.getVersionNumber().toVersionString()));
        }

        public ValueProducer<String> deviceCategory() {
            return new ValueProducer<String>(identifier + ".deviceCategory()", (e,c) -> produce(e,c).map((ua) -> ua.getDeviceCategory().getName()));
        }

        public ValueProducer<String> osFamily() {
            return new ValueProducer<String>(identifier + ".osFamily()", (e,c) -> produce(e,c).map((ua) -> ua.getOperatingSystem().getFamily().getName()));
        }

        public ValueProducer<String> osVersion() {
            return new ValueProducer<String>(identifier + ".osVersion()", (e,c) -> produce(e,c).map((ua) -> ua.getOperatingSystem().getVersionNumber().toVersionString()));
        }

        public ValueProducer<String> osVendor() {
            return new ValueProducer<String>(identifier + ".osVendor()", (e,c) -> produce(e,c).map((ua) -> ua.getOperatingSystem().getProducer()));
        }
    }

    /*
     * Regex mapping
     */
    public ValueProducer<Matcher> matcher(final ValueProducer<String> source, String regex) {
        return new MatcherValueProducer(source, regex);
    }

    public final static class MatcherValueProducer extends ValueProducer<Matcher> {
        private MatcherValueProducer(final ValueProducer<String> source, final String regex) {
            super(
                    "match(" + regex + " against " + source.identifier + ")",
                    (e,c) -> {
                        return source.produce(e,c).map((s) -> Pattern.compile(regex).matcher(s));
                    },
                    true);
        }

        public ValueProducer<Boolean> matches() {
            return new ValueProducer<Boolean>(
                    identifier + ".matches()",
                    (e,c) -> this.produce(e,c).map(Matcher::matches));
        }

        // Note: matches() must be called on a Matcher prior to calling group
        // In case of no match, group(...) throws an exception, in case there is
        // a match, but the group doesn't capture anything, it returns null.
        public ValueProducer<String> group(final int group) {
            return new ValueProducer<>(
                    identifier + ".group(" + group + ")",
                    (e,c) -> this.produce(e,c).map((m) -> m.matches() ? m.group(group) : null));
        }

        public ValueProducer<String> group(final String group) {
            return new ValueProducer<>(
                    identifier + ".group(" + group + ")",
                    (e,c) -> this.produce(e,c).map((m) -> m.matches() ? m.group(group) : null));
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
                    (e,c) -> {
                        return source.produce(e, c).map((location) -> {
                           try {
                                return new URI(location);
                            } catch (Exception ignored) {
                                // When we cannot parse as URI, leave the producer empty
                                return null;
                            }
                        });
                    },
                    true);
        }

        public ValueProducer<String> path() {
            return new ValueProducer<String>(
                    identifier + ".path()",
                    (e,c) -> produce(e,c).map(URI::getPath));
        }

        public ValueProducer<String> rawPath() {
            return new ValueProducer<String>(
                    identifier + ".rawPath()",
                    (e,c) -> produce(e,c).map(URI::getRawPath));
        }

        public ValueProducer<String> scheme() {
            return new ValueProducer<String>(
                    identifier + ".scheme()",
                    (e,c) -> produce(e,c).map(URI::getScheme));
        }

        public ValueProducer<String> host() {
            return new ValueProducer<String>(
                    identifier + ".host()",
                    (e,c) -> produce(e,c).map(URI::getHost));
        }

        public ValueProducer<Integer> port() {
            return new ValueProducer<Integer>(
                    identifier + ".port()",
                    (e,c) -> produce(e,c).map((uri) -> uri.getPort() != -1 ? uri.getPort() : null));
        }

        public ValueProducer<String> decodedQueryString() {
            return new ValueProducer<String>(
                    identifier + ".decodedQueryString()",
                    (e,c) -> produce(e,c).map(URI::getQuery));
        }

        public ValueProducer<String> rawQueryString() {
            return new ValueProducer<String>(
                    identifier + ".rawQueryString()",
                    (e,c) -> produce(e,c).map(URI::getRawQuery));
        }

        public ValueProducer<String> decodedFragment() {
            return new ValueProducer<String>(
                    identifier + ".decodedFragment()",
                    (e,c) -> produce(e,c).map(URI::getFragment));
        }

        public ValueProducer<String> rawFragment() {
            return new ValueProducer<String>(
                    identifier + ".rawFragment()",
                    (e,c) -> produce(e,c).map(URI::getRawFragment));
        }

        public QueryStringValueProducer query() {
            return new QueryStringValueProducer(rawQueryString());
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
            return new ValueProducer<String>(
                    identifier + ".value(" + key + ")",
                    (e,c) -> produce(e,c).map((qs) -> qs.get(key)).map((l) -> l.get(0)));
        }

        public ValueProducer<List<String>> valueList(final String key) {
            return new ValueProducer<List<String>>(
                    identifier + ".valueList(" + key + ")",
                    (e,c) -> produce(e,c).map((qs) -> qs.get(key)));
        }
    }

    /*
     * Cookie mapping
     */
    public ValueProducer<String> cookie(final String name) {
        return new ValueProducer<String>(
                "cookie(" + name + ")",
                (e,c) -> Optional.ofNullable(e.getRequestCookies().get(name)).map(Cookie::getValue));
    }

    /*
     * Custom event parameter mapping
     */
    public ValueProducer<String> eventParameter(final String name) {
        return new ValueProducer<String>(
                "eventParameter(" + name + ")",
                (e, c) -> queryParam(e, EVENT_TYPE_QUERY_PARAM + "." + name));
    }

    /*
     * Custom header mapping
     */
    public HeaderValueProducer header(final String name) {
        return new HeaderValueProducer(name);
    }

    public final static class HeaderValueProducer extends ValueProducer<HeaderValues> {
        private final static Joiner COMMA_JOINER = Joiner.on(',');

        private HeaderValueProducer(final String name) {
            super(
                    "header(" + name + ")",
                    (e,c) -> Optional.ofNullable(e.getRequestHeaders().get(name)));
        }

        public ValueProducer<String> first() {
            return new ValueProducer<String>(
                    identifier + ".first()",
                    (e,c) -> this.produce(e, c).map(HeaderValues::getFirst));
        }

        public ValueProducer<String> last() {
            return new ValueProducer<String>(
                    identifier + ".last()",
                    (e,c) -> this.produce(e, c).map(HeaderValues::getLast));
        }

        public ValueProducer<String> commaSeparated() {
            return new ValueProducer<String>(
                    identifier + ".commaSeparated()",
                    (e,c) -> this.produce(e, c).map(COMMA_JOINER::join));
        }
    }

    /*
     * IP to geo mapping
     */
    public GeoIpValueProducer ip2geo(final ValueProducer<String> source) {
        return new GeoIpValueProducer(
                new ValueProducer<InetAddress>("parse(" + source.identifier + " to IP address)", (e,c) -> source.produce(e, c).flatMap(DslRecordMapping::tryParseIpv4)),
                verifyAndReturnLookupService());
    }

    public GeoIpValueProducer ip2geo() {
        return new GeoIpValueProducer(
                new ValueProducer<InetAddress>("<remote host IP>", (e,c) -> Optional.ofNullable(e.getSourceAddress()).map(InetSocketAddress::getAddress)),
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
            return new ValueProducer<Integer>(
                    identifier + ".cityId()",
                    (e,c) -> produce(e, c).map((r) -> r.getCity()).map(City::getGeoNameId));
        }

        public ValueProducer<String> cityName() {
            return new ValueProducer<String>(
                    identifier + ".cityName()",
                    (e,c) -> produce(e, c).map((r) -> r.getCity()).map(City::getName));
        }

        public ValueProducer<String> continentCode() {
            return new ValueProducer<String>(
                    identifier + ".continentCode()",
                    (e,c) -> produce(e, c).map((r) -> r.getContinent()).map(Continent::getCode));
        }

        public ValueProducer<Integer> continentId() {
            return new ValueProducer<Integer>(
                    identifier + ".continentId()",
                    (e,c) -> produce(e, c).map((r) -> r.getContinent()).map(Continent::getGeoNameId));
        }

        public ValueProducer<String> continentName() {
            return new ValueProducer<String>(
                    identifier + ".continentName()",
                    (e,c) -> produce(e, c).map((r) -> r.getContinent()).map(Continent::getName));
        }

        public ValueProducer<String> countryCode() {
            return new ValueProducer<String>(
                    identifier + ".countryCode()",
                    (e,c) -> produce(e, c).map((r) -> r.getCountry()).map(Country::getIsoCode));
        }

        public ValueProducer<Integer> countryId() {
            return new ValueProducer<Integer>(
                    identifier + ".countryId()",
                    (e,c) -> produce(e, c).map((r) -> r.getCountry()).map(Country::getGeoNameId));
        }

        public ValueProducer<String> countryName() {
            return new ValueProducer<String>(
                    identifier + ".countryName()",
                    (e,c) -> produce(e, c).map((r) -> r.getCountry()).map(Country::getName));
        }

        public ValueProducer<Double> latitude() {
            return new ValueProducer<Double>(
                    identifier + ".latitude()",
                    (e,c) -> produce(e, c).map((r) -> r.getLocation()).map(Location::getLatitude));
        }

        public ValueProducer<Double> longitude() {
            return new ValueProducer<Double>(
                    identifier + ".latitude()",
                    (e,c) -> produce(e, c).map((r) -> r.getLocation()).map(Location::getLongitude));
        }

        public ValueProducer<Integer> metroCode() {
            return new ValueProducer<Integer>(
                    identifier + ".metroCode()",
                    (e,c) -> produce(e, c).map((r) -> r.getLocation()).map(Location::getMetroCode));
        }

        public ValueProducer<String> timeZone() {
            return new ValueProducer<String>(
                    identifier + ".timeZone()",
                    (e,c) -> produce(e, c).map((r) -> r.getLocation()).map(Location::getTimeZone));
        }

        public ValueProducer<String> mostSpecificSubdivisionCode() {
            return new ValueProducer<String>(
                    identifier + ".mostSpecificSubdivisionCode()",
                    (e,c) -> produce(e, c).map((r) -> r.getMostSpecificSubdivision()).map(Subdivision::getIsoCode));
        }

        public ValueProducer<Integer> mostSpecificSubdivisionId() {
            return new ValueProducer<Integer>(
                    identifier + ".mostSpecificSubdivisionId()",
                    (e,c) -> produce(e, c).map((r) -> r.getMostSpecificSubdivision()).map(Subdivision::getGeoNameId));
        }

        public ValueProducer<String> mostSpecificSubdivisionName() {
            return new ValueProducer<String>(
                    identifier + ".mostSpecificSubdivisionName()",
                    (e,c) -> produce(e, c).map((r) -> r.getMostSpecificSubdivision()).map(Subdivision::getName));
        }

        public ValueProducer<String> postalCode() {
            return new ValueProducer<String>(
                    identifier + ".postalCode()",
                    (e,c) -> produce(e, c).map((r) -> r.getPostal()).map(Postal::getCode));
        }

        public ValueProducer<String> registeredCountryCode() {
            return new ValueProducer<String>(
                    identifier + ".registeredCountryCode()",
                    (e,c) -> produce(e, c).map((r) -> r.getRegisteredCountry()).map(Country::getIsoCode));
        }

        public ValueProducer<Integer> registeredCountryId() {
            return new ValueProducer<Integer>(
                    identifier + ".registeredCountryId()",
                    (e,c) -> produce(e, c).map((r) -> r.getRegisteredCountry()).map(Country::getGeoNameId));
        }

        public ValueProducer<String> registeredCountryName() {
            return new ValueProducer<String>(
                    identifier + ".registeredCountryName()",
                    (e,c) -> produce(e, c).map((r) -> r.getRegisteredCountry()).map(Country::getName));
        }

        public ValueProducer<String> representedCountryCode() {
            return new ValueProducer<String>(
                    identifier + ".representedCountryCode()",
                    (e,c) -> produce(e, c).map((r) -> r.getRepresentedCountry()).map(Country::getIsoCode));
        }

        public ValueProducer<Integer> representedCountryId() {
            return new ValueProducer<Integer>(
                    identifier + ".representedCountryId()",
                    (e,c) -> produce(e, c).map((r) -> r.getRepresentedCountry()).map(Country::getGeoNameId));
        }

        public ValueProducer<String> representedCountryName() {
            return new ValueProducer<String>(
                    identifier + ".representedCountryName()",
                    (e,c) -> produce(e, c).map((r) -> r.getRepresentedCountry()).map(Country::getName));
        }

        public ValueProducer<List<String>> subdivisionCodes() {
            return new ValueProducer<List<String>>(
                    identifier + ".subdivisionCodes()",
                    (e,c) -> produce(e, c).map((r) -> r.getSubdivisions()).map((l) -> Lists.transform(l, Subdivision::getIsoCode)));
        }

        public ValueProducer<List<Integer>> subdivisionIds() {
            return new ValueProducer<List<Integer>>(
                    identifier + ".subdivisionIds()",
                    (e,c) -> produce(e, c).map((r) -> r.getSubdivisions()).map((l) -> Lists.transform(l, Subdivision::getGeoNameId)));
        }

        public ValueProducer<List<String>> subdivisionNames() {
            return new ValueProducer<List<String>>(
                    identifier + ".subdivisionNames()",
                    (e,c) -> produce(e, c).map((r) -> r.getSubdivisions()).map((l) -> Lists.transform(l, Subdivision::getName)));
        }

        public ValueProducer<Integer> autonomousSystemNumber() {
            return new ValueProducer<Integer>(
                    identifier + ".autonomousSystemNumber()",
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::getAutonomousSystemNumber));
        }

        public ValueProducer<String> autonomousSystemOrganization() {
            return new ValueProducer<String>(
                    identifier + ".autonomousSystemOrganization()",
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::getAutonomousSystemOrganization));
        }

        public ValueProducer<String> domain() {
            return new ValueProducer<String>(
                    identifier + ".domain()",
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::getDomain));
        }

        public ValueProducer<String> isp() {
            return new ValueProducer<String>(
                    identifier + ".isp()",
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::getIsp));
        }

        public ValueProducer<String> organisation() {
            return new ValueProducer<String>(
                    identifier + ".organisation()",
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::getOrganization));
        }

        public ValueProducer<Boolean> anonymousProxy() {
            return new ValueProducer<Boolean>(
                    identifier + ".anonymousProxy()",
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::isAnonymousProxy));
        }

        public ValueProducer<Boolean> satelliteProvider() {
            return new ValueProducer<Boolean>(
                    identifier + ".satelliteProvider()",
                    (e,c) -> produce(e, c).map((r) -> r.getTraits()).map(Traits::isSatelliteProvider));
        }
    }

    /*
     * Handy when using something other than the remote host as IP address (e.g. custom headers set by a load balancer).
     * We force the address to be in the numeric IPv4 scheme, to prevent name resolution and IPv6.
     */
    private static Optional<InetAddress> tryParseIpv4(String ip) {
        byte[] result = new byte[4];
        String[] parts = StringUtils.split(ip, '.');
        if (parts.length != 4) {
            return Optional.empty();
        }
        try {
            for (int c = 0; c < 4; c++) {
                int x = Integer.parseInt(parts[c]);
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

    /*
     * Internal methods and types
     */
    private static Optional<String> queryParam(final HttpServerExchange exchange, final String param) {
        return Optional.ofNullable(exchange.getQueryParameters().get(param)).map(Deque::getFirst);
    }

    private static class ValueProducer<T> {
        private final BiFunction<HttpServerExchange, Map<String,Object>, Optional<T>> supplier;
        private final boolean memoize;
        protected final String identifier;

        public ValueProducer(final String identifier, final BiFunction<HttpServerExchange, Map<String,Object>, Optional<T>> supplier, boolean memoize) {
            this.identifier = identifier;
            this.supplier = supplier;
            this.memoize = memoize;
        }

        public ValueProducer(final String readableName, final BiFunction<HttpServerExchange, Map<String,Object>, Optional<T>> supplier) {
            this(readableName, supplier, false);
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
            return new ValueProducer<Boolean>(
                    identifier + ".equalTo(" + other.identifier + ")",
                    (e, c) -> {
                        Optional<T> left = this.produce(e, c);
                        Optional<T> right = other.produce(e, c);
                        return left.isPresent() && right.isPresent() ? Optional.of(left.get().equals(right.get())) : Optional.of(false);
                    });
        }

        public ValueProducer<Boolean> equalTo(final T other) {
            return new ValueProducer<Boolean>(
                    identifier + ".equalTo(" + other + ")",
                    (e, c) -> {
                        Optional<T> left = this.produce(e, c);
                        return left.isPresent() ? Optional.of(left.get().equals(other)) : Optional.of(false);
                    });
        }

        public ValueProducer<Boolean> isPresent() {
            return new ValueProducer<Boolean>(
                    this.identifier + ".isPresent()",
                    (e,c) -> Optional.of(produce(e, c).map((x) -> Boolean.TRUE).orElse(Boolean.FALSE)));
        }

        public ValueProducer<Boolean> isAbsent() {
            return new ValueProducer<Boolean>(
                    this.identifier + ".isAbsent()",
                    (e,c) -> Optional.of(produce(e, c).map((x) -> Boolean.FALSE).orElse(Boolean.TRUE)));
        }

        @Override
        public String toString() {
            return identifier;
        }
    }

    static interface MappingAction {
        void perform(HttpServerExchange echange, Map<String,Object> context, GenericRecordBuilder record);
    }
}
