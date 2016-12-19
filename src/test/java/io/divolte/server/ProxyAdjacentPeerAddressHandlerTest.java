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

import static org.junit.Assert.*;
import io.divolte.server.ServerTestUtils.EventPayload;
import io.divolte.server.ServerTestUtils.TestServer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public class ProxyAdjacentPeerAddressHandlerTest {
    private static final String URL_STRING = "http://localhost:%d/csc-event";
    private static final String URL_QUERY_STRING = "?"
            + "p=0%3Ai0rjfnxc%3AJLOvH9Nda2c1uV8M~vmdhPGFEC3WxVNq&"
            + "s=0%3Ai0rjfnxc%3AFPpXFMdcEORvvaP_HbpDgABG3Iu5__4d&"
            + "v=0%3AOxVC1WJ4PZNEGIUuzdXPsy_bztnKMuoH&"
            + "e=0%3AOxVC1WJ4PZNEGIUuzdXPsy_bztnKMuoH0&"
            + "c=i0rjfnxd&"
            + "n=t&"
            + "f=t&"
            + "l=http%3A%2F%2Fwww.example.com%2F&"
            + "i=sg&"
            + "j=sg&"
            + "k=1&"
            + "w=sg&"
            + "h=sg&"
            + "t=pageView";

    private TestServer server;

    @Test
    public void shouldObtainRightMostAddressFromChain() throws IOException, InterruptedException {
        final URL url = new URL(String.format(URL_STRING, server.port) + URL_QUERY_STRING);
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.addRequestProperty("X-Forwarded-For", "127.0.0.1,192.168.13.23");
        conn.setRequestMethod("GET");

        assertEquals(200, conn.getResponseCode());

        final EventPayload payload = server.waitForEvent();
        assertEquals("192.168.13.23", payload.event.exchange.getSourceAddress().getHostString());
    }

    @Test
    public void shouldObtainSingleValueWithoutCommas() throws IOException, InterruptedException {
        final URL url = new URL(String.format(URL_STRING, server.port) + URL_QUERY_STRING);
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.addRequestProperty("X-Forwarded-For", "127.0.0.1");
        conn.setRequestMethod("GET");

        assertEquals(200, conn.getResponseCode());

        final EventPayload payload = server.waitForEvent();
        assertEquals("127.0.0.1", payload.event.exchange.getSourceAddress().getHostString());
    }

    @Test
    public void shouldAllowWhitespaceAfterComma() throws IOException, InterruptedException {
        final URL url = new URL(String.format(URL_STRING, server.port) + URL_QUERY_STRING);
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.addRequestProperty("X-Forwarded-For", "127.0.0.1, 192.168.13.23");
        conn.setRequestMethod("GET");

        assertEquals(200, conn.getResponseCode());

        final EventPayload payload = server.waitForEvent();
        assertEquals("192.168.13.23", payload.event.exchange.getSourceAddress().getHostString());
    }

    @Test
    public void shouldAllowMultipleXffHeaders() throws IOException, InterruptedException {
        final URL url = new URL(String.format(URL_STRING, server.port) + URL_QUERY_STRING);
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.addRequestProperty("X-Forwarded-For", "127.0.0.1, 8.8.8.8");
        conn.addRequestProperty("X-Forwarded-For", "192.168.13.23");
        conn.setRequestMethod("GET");

        assertEquals(200, conn.getResponseCode());

        final EventPayload payload = server.waitForEvent();
        assertEquals("192.168.13.23", payload.event.exchange.getSourceAddress().getHostString());
    }

    @Before
    public void setUp() {
        server = new TestServer("x-forwarded-for-test.conf");
    }

    @After
    public void tearDown() {
        server.server.shutdown();
    }
}
