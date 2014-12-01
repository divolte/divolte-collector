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
import static org.junit.Assert.*;
import io.divolte.server.ServerTestUtils.EventPayload;
import io.divolte.server.ServerTestUtils.TestServer;
import io.undertow.server.HttpServerExchange;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class DslRecordMapperTest {
    private static final String DIVOLTE_URL_STRING = "http://localhost:%d/csc-event";
    private static final String DIVOLTE_URL_QUERY_STRING = "?"
            + "p=0%3Ai0rjfnxc%3AJLOvH9Nda2c1uV8M~vmdhPGFEC3WxVNq&"
            + "s=0%3Ai0rjfnxc%3AFPpXFMdcEORvvaP_HbpDgABG3Iu5__4d&"
            + "v=0%3AOxVC1WJ4PZNEGIUuzdXPsy_bztnKMuoH&"
            + "e=0%3AOxVC1WJ4PZNEGIUuzdXPsy_bztnKMuoH0&"
            + "c=i0rjfnxd&"
            + "n=t&"
            + "f=t&"
            + "i=sg&"
            + "j=sg&"
            + "k=2&"
            + "w=sa&"
            + "h=sa&"
            + "t=pageView";

    private static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36";

    public TestServer server;
    private File mappingFile;
    private File avroFile;

    @Test
    public void shouldPopulateFlatFields() throws InterruptedException, IOException {
        setupServer("flat-mapping.groovy");

        EventPayload event = request("https://example.com/", "http://example.com/");
        final GenericRecord record = event.record;
        final HttpServerExchange exchange = event.exchange;

        assertEquals(true, record.get("sessionStart"));
        assertEquals(exchange.getAttachment(REQUEST_START_TIME_KEY), record.get("ts"));
        assertEquals("https://example.com/", record.get("location"));
        assertEquals("http://example.com/", record.get("referer"));

        assertEquals(USER_AGENT, record.get("userAgentString"));
        assertEquals("Chrome", record.get("userAgentName"));
        assertEquals("Chrome", record.get("userAgentFamily"));
        assertEquals("Google Inc.", record.get("userAgentVendor"));
        assertEquals("Browser", record.get("userAgentType"));
        assertEquals("38.0.2125.122", record.get("userAgentVersion"));
        assertEquals("Personal computer", record.get("userAgentDeviceCategory"));
        assertEquals("OS X", record.get("userAgentOsFamily"));
        assertEquals("10.10.1", record.get("userAgentOsVersion"));
        assertEquals("Apple Computer, Inc.", record.get("userAgentOsVendor"));

        assertEquals(exchange.getAttachment(PARTY_COOKIE_KEY).value, record.get("client"));
        assertEquals(exchange.getAttachment(SESSION_COOKIE_KEY).value, record.get("session"));
        assertEquals(exchange.getAttachment(PAGE_VIEW_ID_KEY), record.get("pageview"));
        assertEquals(exchange.getAttachment(EVENT_ID_KEY), record.get("event"));
        assertEquals(1018, record.get("viewportWidth"));
        assertEquals(1018, record.get("viewportHeight"));
        assertEquals(1024, record.get("screenWidth"));
        assertEquals(1024, record.get("screenHeight"));
        assertEquals(2, record.get("pixelRatio"));
        assertEquals("pageView", record.get("eventType"));

        Stream.of("sessionStart",
                "ts",
                "location",
                "referer",
                "userAgentString",
                "userAgentName",
                "userAgentFamily",
                "userAgentVendor",
                "userAgentType",
                "userAgentVersion",
                "userAgentDeviceCategory",
                "userAgentOsFamily",
                "userAgentOsVersion",
                "userAgentOsVendor",
                "client",
                "session",
                "pageview",
                "event",
                "viewportWidth",
                "viewportHeight",
                "screenWidth",
                "screenHeight",
                "pixelRatio",
                "eventType")
              .forEach((v) -> assertNotNull(record.get(v)));
    }

    private EventPayload request(String location) throws IOException, InterruptedException {
        return request(location, null);
    }

    private EventPayload request(String location, String referer) throws IOException, InterruptedException {
        final URL url = referer == null ?
                new URL(
                        String.format(DIVOLTE_URL_STRING, server.port) +
                        DIVOLTE_URL_QUERY_STRING +
                        "&l=" + URLEncoder.encode(location, StandardCharsets.UTF_8.name()))
                :
                new URL(
                        String.format(DIVOLTE_URL_STRING, server.port) +
                        DIVOLTE_URL_QUERY_STRING +
                        "&l=" + URLEncoder.encode(location, StandardCharsets.UTF_8.name()) +
                        "&r=" + URLEncoder.encode(referer, StandardCharsets.UTF_8.name()));

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.addRequestProperty("User-Agent", USER_AGENT);
        conn.setRequestMethod("GET");

        assertEquals(200, conn.getResponseCode());

        return server.waitForEvent();
    }

    private void setupServer(final String mapping) throws IOException {
        mappingFile = File.createTempFile("test-mapping", ".groovy");
        Files.write(Resources.toByteArray(Resources.getResource(mapping)), mappingFile);

        avroFile = File.createTempFile("TestSchema-", ".avsc");
        Files.write(Resources.toByteArray(Resources.getResource("TestRecord.avsc")), avroFile);

        ImmutableMap<String, Object> mappingConfig = ImmutableMap.of(
                "divolte.tracking.schema_mapping.mapping_script_file", mappingFile.getAbsolutePath(),
                "divolte.tracking.schema_file", avroFile.getAbsolutePath()
                );

        server = new TestServer("dsl-mapping-test.conf", mappingConfig);
        server.server.run();
    }

    @After
    public void shutdown() {
        server.server.shutdown();
        mappingFile.delete();
        avroFile.delete();
    }
}
