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

import com.google.common.base.Preconditions;
import io.divolte.server.ServerTestUtils.EventPayload;
import io.divolte.server.ServerTestUtils.TestServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static io.divolte.server.IncomingRequestProcessor.DIVOLTE_EVENT_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@ParametersAreNonnullByDefault
public class RequestChecksumTest {
    private static final String URL_STRING = "http://localhost:%d/csc-event";

    private static final String URL_QUERY_CHECKSUM_MISSING = '?'
            + "p=0%3Ai1t84hgy%3A5AF359Zjq5kUy98u4wQjlIZzWGhN~GlG&"
            + "s=0%3Ai1t84hgy%3A95CbiPCYln_1e0a6rFvuRkDkeNnc6KC8&"
            + "v=0%3A1fF6GFGjDOQiEx_OxnTm_tl4BH91eGLF&"
            + "e=0%3A1fF6GFGjDOQiEx_OxnTm_tl4BH91eGLF0&"
            + "c=i1t8q2b6&"
            + "n=f&"
            + "f=f&"
            + "l=http%3A%2F%2Flocalhost%3A8290%2F&"
            + "i=1ak&"
            + "j=sj&"
            + "k=2&"
            + "w=uq&"
            + "h=qd&"
            + "t=pageView";
    private static final String URL_QUERY_CHECKSUM_GOOD = URL_QUERY_CHECKSUM_MISSING + "&x=si9804";
    private static final String URL_QUERY_CHECKSUM_BAD = URL_QUERY_CHECKSUM_MISSING + "&x=si9805";
    private static final String[] URL_QUERY_CHECKSUM_PARTIALS = {
            URL_QUERY_CHECKSUM_MISSING + "&x",
            URL_QUERY_CHECKSUM_MISSING + "&x=",
    };
    private static final String URL_QUERY_CHECKSUM_UNICODE = '?'
            + "p=0%3Ai1t84hgy%3Aparty&"
            + "s=0%3Ai1t84hgy%3Asession&"
            + "v=0%3ApageView&"
            + "e=0%3AeventId&"
            + "c=i1t8q2b6&"
            + "n=f&"
            + "f=f&"
            + "l=http%3A%2F%2Flocalhost%3A8290%2F&"
            + "i=1ak&"
            + "j=sj&"
            + "k=2&"
            + "w=uq&"
            + "h=qd&"
            + "t=%E1%BB%A5%C3%B1%E2%9A%95%C2%A9%C2%BA%E1%B8%8C%E2%84%A8&"
            + "x=-ql2p2c";

    private static final String URL_QUERY_SENTINEL = '?'
            + "p=0%3Ai1t84hgy%3Aparty&"
            + "s=0%3Ai1t84hgy%3Asession&"
            + "v=0%3ApageView&"
            + "e=0%3AeventId&"
            + "c=i1t8q2b6&"
            + "n=f&"
            + "f=f&"
            + "l=http%3A%2F%2Flocalhost%3A8290%2F&"
            + "i=1ak&"
            + "j=sj&"
            + "k=2&"
            + "w=uq&"
            + "h=qd&"
            + "t=sentinelEvent&"
            + "x=-y99lem";

    private String serverConfigurationResourceName;

    @Nullable
    private TestServer server;

    @Test
    public void shouldFlagCorrectChecksumAsNotCorrupted() throws IOException, InterruptedException {
        request(URL_QUERY_CHECKSUM_GOOD);
        Preconditions.checkState(null != server);
        final EventPayload event = server.waitForEvent();
        assertFalse(event.exchange.getAttachment(DIVOLTE_EVENT_KEY).corruptEvent);
    }

    @Test
    public void shouldFlagIncorrectChecksumAsCorrupted() throws IOException, InterruptedException {
        request(URL_QUERY_CHECKSUM_BAD);
        Preconditions.checkState(null != server);
        final EventPayload event = server.waitForEvent();
        assertTrue(event.exchange.getAttachment(DIVOLTE_EVENT_KEY).corruptEvent);
    }

    @Test
    public void shouldFlagMissingChecksumAsCorrupted() throws IOException, InterruptedException {
        request(URL_QUERY_CHECKSUM_MISSING);
        Preconditions.checkState(null != server);
        final EventPayload event = server.waitForEvent();
        assertTrue(event.exchange.getAttachment(DIVOLTE_EVENT_KEY).corruptEvent);
    }

    @Test
    public void shouldFlagPartialChecksumAsCorrupted() throws IOException, InterruptedException {
        for (final String urlQueryChecksumPartial : URL_QUERY_CHECKSUM_PARTIALS) {
            request(urlQueryChecksumPartial);
            Preconditions.checkState(null != server);
            final EventPayload event = server.waitForEvent();
            assertTrue(event.exchange.getAttachment(DIVOLTE_EVENT_KEY).corruptEvent);
        }
    }

    @Test
    public void shouldChecksumCorrectlyWithNonAsciiParameters() throws IOException, InterruptedException {
        request(URL_QUERY_CHECKSUM_UNICODE);
        Preconditions.checkState(null != server);
        final EventPayload event = server.waitForEvent();
        final DivolteEvent eventData = event.exchange.getAttachment(DIVOLTE_EVENT_KEY);
        assertFalse(eventData.corruptEvent);
        assertEquals("ụñ⚕©ºḌℨ", eventData.eventType.get());
    }

    @Test
    public void shouldDiscardCorruptedEventsIfConfigured() throws InterruptedException, IOException {
        serverConfigurationResourceName = "checksum-discard-corrupt-test.conf";
        request(URL_QUERY_CHECKSUM_BAD);
        request(URL_QUERY_SENTINEL);
        Preconditions.checkState(null != server);
        final EventPayload event = server.waitForEvent();
        // The first request should be missing, and we should now have the sentinel event.
        final String eventType = event.exchange.getAttachment(DIVOLTE_EVENT_KEY).eventType.get();
        assertEquals("sentinelEvent", eventType);
    }

    private void request(final String queryString) throws IOException {
        setServerConf(serverConfigurationResourceName);
        Preconditions.checkState(null != server);
        final URL url = new URL(String.format(URL_STRING, server.port) + queryString);
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        assertEquals(200, conn.getResponseCode());
    }

    private void setServerConf(final String configurationResourceName) {
        if (null == server || !configurationResourceName.equals(server.config.origin().resource())) {
            setServer(new TestServer(configurationResourceName));
        }
    }

    @Before
    public void setUp() {
        serverConfigurationResourceName = "checksum-test.conf";
    }

    @After
    public void tearDown() {
        setServer(null);
    }

    private void setServer(@Nullable TestServer newServer) {
        final TestServer oldServer = this.server;
        if (oldServer != newServer) {
            if (null != oldServer) {
                oldServer.server.shutdown();
            }
            this.server = newServer;
            if (null != newServer) {
                newServer.server.run();
            }
        }
    }
}
