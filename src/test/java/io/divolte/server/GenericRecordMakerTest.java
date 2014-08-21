package io.divolte.server;

import io.divolte.server.CookieValues.CookieValue;
import io.divolte.server.GenericRecordMaker.SchemaMappingException;
import io.divolte.server.geo2ip.LookupService;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.CookieImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.maxmind.geoip2.model.CityResponse;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import static io.divolte.server.DivolteEventHandler.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@ParametersAreNonnullByDefault
public class GenericRecordMakerTest {
    @Rule
    public ExpectedException expected = ExpectedException.none();


    @Test
    public void shouldPopulateFlatFields() throws IOException, UnirestException {
        Schema schema = schemaFromClassPath("/TestRecord.avsc");
        Config config = ConfigFactory.load("schema-test-flatfields");

        GenericRecordMaker maker = new GenericRecordMaker(schema, config, ConfigFactory.load(), Optional.empty());

        setupExchange(
                "Divolte/Test",
                "p=the_page_view_id",
                "l=https://example.com/",
                "r=http://example.com/",
                "i=1024",
                "j=768",
                "w=640",
                "h=480"
                );

        GenericRecord record = maker.makeRecordFromExchange(theExchange);

        assertEquals(true, record.get("sessionStart"));
        assertEquals(theExchange.getAttachment(REQUEST_START_TIME_KEY), record.get("ts"));
        assertEquals("https://example.com/", record.get("location"));
        assertEquals("http://example.com/", record.get("referer"));
        assertEquals("Divolte/Test", record.get("userAgentString"));
        assertEquals(theExchange.getAttachment(PARTY_COOKIE_KEY).value, record.get("client"));
        assertEquals(theExchange.getAttachment(SESSION_COOKIE_KEY).value, record.get("session"));
        assertEquals(theExchange.getAttachment(PAGE_VIEW_ID_KEY), record.get("pageview"));
        assertEquals(640, record.get("viewportWidth"));
        assertEquals(480, record.get("viewportHeight"));
    }

    @Test
    public void shouldParseUserAgentString() throws IOException, UnirestException {
        Schema schema = schemaFromClassPath("/TestRecord.avsc");
        Config config = ConfigFactory.load("schema-test-useragent");

        GenericRecordMaker maker = new GenericRecordMaker(schema, config, ConfigFactory.load(), Optional.empty());

        String ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36";

        setupExchange(ua);
        GenericRecord record = maker.makeRecordFromExchange(theExchange);

        assertEquals("Chrome", record.get("userAgentName"));
        assertEquals("Chrome", record.get("userAgentFamily"));
        assertEquals("Google Inc.", record.get("userAgentVendor"));
        assertEquals("Browser", record.get("userAgentType"));
        assertEquals("36.0.1985.125", record.get("userAgentVersion"));
        assertEquals("Personal computer", record.get("userAgentDeviceCategory"));
        assertEquals("OS X", record.get("userAgentOsFamily"));
        assertEquals("10.9.4", record.get("userAgentOsVersion"));
        assertEquals("Apple Computer, Inc.", record.get("userAgentOsVendor"));
    }

    @Test
    public void shouldFailOnUnsupportedVersion() throws IOException {
        expected.expect(SchemaMappingException.class);
        expected.expectMessage("Unsupported schema mapping configuration version: 42");
        Schema schema = schemaFromClassPath("/TestRecord.avsc");
        Config config = ConfigFactory.load("schema-wrong-version");
        new GenericRecordMaker(schema, config, ConfigFactory.load(), Optional.empty());
    }

    @Test
    public void shouldSetCustomCookieValue() throws IOException, UnirestException {
        Schema schema = schemaFromClassPath("/TestRecord.avsc");
        Config config = ConfigFactory.load("schema-test-customcookie");
        GenericRecordMaker maker = new GenericRecordMaker(schema, config, ConfigFactory.load(), Optional.empty());

        setupExchange("Divolte/Test");
        GenericRecord record = maker.makeRecordFromExchange(theExchange);

        assertEquals("custom_cookie_value", record.get("customCookie"));
    }

    @Test
    public void shouldSetFieldWithMatchingRegexName() throws IOException, UnirestException {
        Schema schema = schemaFromClassPath("/TestRecord.avsc");
        Config config = ConfigFactory.load("schema-test-matchingregex");
        GenericRecordMaker maker = new GenericRecordMaker(schema, config, ConfigFactory.load(), Optional.empty());

        setupExchange("Divolte/Test", "l=http://example.com/", "r=https://www.example.com/bla/");
        GenericRecord record = maker.makeRecordFromExchange(theExchange);

        assertEquals("http", record.get("locationProtocol"));
        assertEquals("https", record.get("refererProtocol"));
    }

    @Test
    public void shouldSetFieldWithCaptureGroupFromRegex() throws IOException, UnirestException {
        Schema schema = schemaFromClassPath("/TestRecord.avsc");
        Config config = ConfigFactory.load("schema-test-regex");
        GenericRecordMaker maker = new GenericRecordMaker(schema, config, ConfigFactory.load(), Optional.empty());

        setupExchange(
                "Divolte/Test",
                "l=http://example.com/part1/part2/part3/ABA_C12_X3B",
                "r=https://www.example.com/about.html");
        GenericRecord record = maker.makeRecordFromExchange(theExchange);

        assertEquals("ABA", record.get("toplevelCategory"));
        assertEquals("C12", record.get("subCategory"));
        assertEquals("about", record.get("contentPage"));
    }

    private void testMapping(final Optional<CityResponse> response,
                             final Map<String,Object> expectedMapping)
            throws IOException, UnirestException {
        // Set up the test.
        final Schema schema = schemaFromClassPath("/TestRecord.avsc");
        final Config config = ConfigFactory.load("schema-test-geo");
        final LookupService mockLookupService = mock(LookupService.class);
        when(mockLookupService.lookup(any())).thenReturn(response);
        setupExchange("Arbitrary User Agent");

        // Perform a mapping.
        final GenericRecordMaker maker = new GenericRecordMaker(schema, config, ConfigFactory.load(), Optional.of(mockLookupService));
        final GenericRecord record = maker.makeRecordFromExchange(theExchange);

        // Validate the results.
        verify(mockLookupService).lookup(any());
        verifyNoMoreInteractions(mockLookupService);
        expectedMapping.forEach((k, v) -> {
            final Object recordValue = record.get(k);
            assertEquals("Property " + k + " not mapped correctly.", v, recordValue);
        });
    }

    @Test
    public void shouldMapAllGeoIpFields() throws IOException, UnirestException {
        final CityResponse mockResponseWithEverything = loadFromClassPath("/city-response-with-everything.json",
                                                                          new TypeReference<CityResponse>(){});
        final Map<String,Object> expectedMapping = loadFromClassPath("/city-response-expected-mapping.json",
                                                                     new TypeReference<Map<String,Object>>(){});
        testMapping(Optional.of(mockResponseWithEverything), expectedMapping);
    }

    private Map<String,Object> buildEmptyMapping(@Nullable final Boolean nullBoolean,
                                                 @Nullable final List<?> nullList) throws IOException {
        // The empty mapping is the same as the full one, except that all values should be null.
        final Map<String,Object> mapping = loadFromClassPath("/city-response-expected-mapping.json",
                                                             new TypeReference<Map<String,Object>>(){});
        for (final Map.Entry<String,Object> entry : mapping.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Boolean) {
                value = nullBoolean;
            } else if (value instanceof List<?>) {
                value = nullList;
            } else {
                value = null;
            }
            entry.setValue(value);
        }
        return mapping;
    }

    @Test
    public void shouldMapMissingGeoIpFields() throws IOException, UnirestException {
        final CityResponse mockResponseWithNothing = MAPPER.readValue("{}",CityResponse.class);
        final Map<String,Object> expectedMapping = buildEmptyMapping(false, ImmutableList.of());
        testMapping(Optional.of(mockResponseWithNothing), expectedMapping);
    }

    @Test
    public void shouldNotPerformGeoLookupIfMappingsDoNotUseIt() throws IOException, UnirestException {
        // Set up the test.
        final Schema schema = schemaFromClassPath("/TestRecord.avsc");
        final Config config = ConfigFactory.load("schema-test-no-geo");
        final LookupService mockLookupService = mock(LookupService.class);
        when(mockLookupService.lookup(any())).thenReturn(Optional.empty());
        setupExchange("Arbitrary User Agent");

        // Perform a mapping.
        new GenericRecordMaker(schema, config, ConfigFactory.load(), Optional.of(mockLookupService));

        // Verify the lookup service was not invoked.
        verify(mockLookupService, never()).lookup(any());
    }

    @Test
    public void shouldNotSetAnyGeoFieldsWhenLookupYieldsNoResult() throws IOException, UnirestException {
        testMapping(Optional.empty(), buildEmptyMapping(null, null));
    }

    private Schema schemaFromClassPath(final String resource) throws IOException {
        try (final InputStream resourceStream = this.getClass().getResourceAsStream(resource)) {
            return new Schema.Parser().parse(resourceStream);
        }
    }

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
                    .setInjectableValues(new InjectableValues.Std().addValue("locales", ImmutableList.of("en")));

    private <T> T loadFromClassPath(final String resource, final TypeReference typeReference) throws IOException {
        try (final InputStream resourceStream = this.getClass().getResourceAsStream(resource)) {
            return MAPPER.readValue(resourceStream, typeReference);
        }
    }

    /*
     * HttpServerExchange is final. In order to construct one, we actually start Undertow,
     * fire a request and have a handler assign the resulting HttpServerExchange to a
     * static field in the test class for further usage.
     *
     * There is no other way...
     */
    private static HttpServerExchange theExchange;
    private static Undertow server;
    private void setupExchange(
            String userAgent,
            String... query
            ) throws UnirestException {

        String queryString = Stream.of(query)
        .map((q) ->  q.split("=", 2))
        .map((q) -> q[0] + "=" + encodeWithoutBitching(q[1]))
        .collect(Collectors.joining("&"));

        Unirest.get(
                String.format("http://localhost:1234/whatever/happens/is/fine%s", "".equals(queryString) ? "" : "?" + queryString))
                .header("accept", "text/plain")
                .header("User-Agent", userAgent)
                .header("Cookie", "custom_cookie=custom_cookie_value;")
                .asString();
    }

    private String encodeWithoutBitching(String source) {
        try {
            return URLEncoder.encode(source, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return source; //best effort, you're on your own
        }
    }

    @BeforeClass
    public static void startUndertow() {
        server = Undertow.builder()
                .setIoThreads(2)
                .setWorkerThreads(1)
                .addHttpListener(1234, "localhost")
                .setHandler((exchange) -> {
                    final long theTime = 42;
                    CookieValue party = CookieValues.generate(theTime);
                    CookieValue session = CookieValues.generate(theTime);
                    CookieValue page = CookieValues.generate(theTime);

                    exchange.putAttachment(REQUEST_START_TIME_KEY, theTime);
                    exchange.putAttachment(PARTY_COOKIE_KEY, party);
                    exchange.putAttachment(SESSION_COOKIE_KEY, session);
                    exchange.putAttachment(PAGE_VIEW_ID_KEY, page.value);

                    exchange.getResponseCookies().put("_dvp", new CookieImpl("_dvp", party.value));
                    exchange.getResponseCookies().put("_dvs", new CookieImpl("_dvs", session.value));
                    exchange.getResponseCookies().put("_dvv", new CookieImpl("_dvv", page.value));

                    exchange.getResponseSender().send("OK");
                    theExchange = exchange;
                })
                .build();
        server.start();
    }

    @AfterClass
    public static void shutdownUndertow() {
        server.stop();
    }
}
