package io.divolte.server;

import static org.junit.Assert.*;
import io.divolte.server.GenericRecordMaker.SchemaMappingException;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.CookieImpl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class GenericRecordMakerTest {
    @Rule
    public ExpectedException expected = ExpectedException.none();


    @Test
    public void shouldPopulateFlatFields() throws IOException, UnirestException {
        Schema schema = schemaFromClassPath("/TestRecord.avsc");
        Config config = ConfigFactory.load("schema-test-flatfields");

        GenericRecordMaker maker = new GenericRecordMaker(schema, config);

        setupExchange(
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
        assertEquals(theExchange.getRequestStartTime(), record.get("ts"));
        assertEquals("https://example.com/", record.get("location"));
        assertEquals("http://example.com/", record.get("referer"));
        assertEquals("Divolte/Test", record.get("userAgentString"));
        assertEquals("party_id_cookie_value", record.get("client"));
        assertEquals("session_id_cookie_value", record.get("session"));
        assertEquals("page_view_id_cookie_value", record.get("pageview"));
        assertEquals(640, record.get("viewportWidth"));
        assertEquals(480, record.get("viewportHeight"));
    }

    @Test
    public void shouldFailOnUnsupportedVersion() throws IOException {
        expected.expect(SchemaMappingException.class);
        expected.expectMessage("Unsupported schema mapping configuration version: 42");
        Schema schema = schemaFromClassPath("/TestRecord.avsc");
        Config config = ConfigFactory.load("schema-wrong-version");
        new GenericRecordMaker(schema, config);
    }

    @Test
    public void shouldSetCustomCookieValue() throws IOException, UnirestException {
        Schema schema = schemaFromClassPath("/TestRecord.avsc");
        Config config = ConfigFactory.load("schema-test-customcookie");
        GenericRecordMaker maker = new GenericRecordMaker(schema, config);

        setupExchange();
        GenericRecord record = maker.makeRecordFromExchange(theExchange);

        assertEquals("custom_cookie_value", record.get("customCookie"));
    }

    @Test
    public void shouldSetFieldWithMatchingRegexName() throws IOException, UnirestException {
        Schema schema = schemaFromClassPath("/TestRecord.avsc");
        Config config = ConfigFactory.load("schema-test-matchingregex");
        GenericRecordMaker maker = new GenericRecordMaker(schema, config);

        setupExchange("l=http://example.com/", "r=https://www.example.com/bla/");
        GenericRecord record = maker.makeRecordFromExchange(theExchange);

        assertEquals("http", record.get("locationProtocol"));
        assertEquals("https", record.get("refererProtocol"));
    }

    @Test
    public void shouldSetFieldWithCaptureGroupFromRegex() throws IOException, UnirestException {
        Schema schema = schemaFromClassPath("/TestRecord.avsc");
        Config config = ConfigFactory.load("schema-test-regex");
        GenericRecordMaker maker = new GenericRecordMaker(schema, config);

        setupExchange(
                "l=http://example.com/part1/part2/part3/ABA_C12_X3B",
                "r=https://www.example.com/about.html");
        GenericRecord record = maker.makeRecordFromExchange(theExchange);

        assertEquals("ABA", record.get("toplevelCategory"));
        assertEquals("C12", record.get("subCategory"));
        assertEquals("about", record.get("contentPage"));
    }

    private Schema schemaFromClassPath(final String resource) throws IOException {
        return new Schema.Parser().parse(this.getClass().getResourceAsStream(resource));
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
            String... query
            ) throws UnirestException {

        String queryString = Stream.of(query)
        .map((q) ->  q.split("=", 2))
        .map((q) -> q[0] + "=" + encodeWithoutBitching(q[1]))
        .collect(Collectors.joining("&"));

        Unirest.get(
                String.format("http://localhost:1234/whatever/happens/is/fine%s", "".equals(queryString) ? "" : "?" + queryString))
                .header("accept", "text/plain")
                .header("User-Agent", "Divolte/Test")
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
                .setServerOption(UndertowOptions.RECORD_REQUEST_START_TIME, true)
                .addHttpListener(1234, "localhost")
                .setHandler((exchange) -> {
                    exchange.getResponseCookies().put("_dvp", new CookieImpl("_dvp", "party_id_cookie_value"));
                    exchange.getResponseCookies().put("_dvs", new CookieImpl("_dvs", "session_id_cookie_value"));
                    exchange.getResponseCookies().put("_dvv", new CookieImpl("_dvv", "page_view_id_cookie_value"));
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
