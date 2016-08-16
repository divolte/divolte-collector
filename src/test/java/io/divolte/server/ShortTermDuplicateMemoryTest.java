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

import static io.divolte.server.IncomingRequestProcessor.*;
import static org.junit.Assert.*;
import io.divolte.server.ServerTestUtils.EventPayload;
import io.divolte.server.ServerTestUtils.TestServer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ShortTermDuplicateMemoryTest {
    private static final String URL_STRING = "http://localhost:%d/csc-event";

    /*
     * CAREFUL! The difference between the first and second query string are chosen,
     * so they hash to different buckets in a memory array of length 2.
     */
    private static final String[] URL_QUERY_STRINGS = {
            "?"
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
            + "t=pageView",
            "?"
            + "p=0%3Ai0rjfnxc%3AJLfhe7Nda2c1uV8M~vmdiPGFEC3WxVNq&" // this one is different from above
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
            + "t=pageView",
            "?"
            + "p=0%3Ai0rjfnxc%3AFDfhe7Nda2c1uV8M~vmdiPGFEC3WxVNq&" // this one is different from above
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
            + "t=pageView"
    };

    private TestServer server;

    @Test
    public void shouldFlagDuplicateRequests() throws IOException, InterruptedException {
        EventPayload payload;

        request(0);
        payload = server.waitForEvent();
        assertEquals(false, payload.event.exchange.getAttachment(DUPLICATE_EVENT_KEY));

        request(1);
        payload = server.waitForEvent();
        assertEquals(false, payload.event.exchange.getAttachment(DUPLICATE_EVENT_KEY));

        request(0);
        payload = server.waitForEvent();
        assertEquals(true, payload.event.exchange.getAttachment(DUPLICATE_EVENT_KEY));
    }

    @Test
    public void shouldForgetAboutRequestHashesAfterSomeTime() throws IOException, InterruptedException {
        EventPayload payload;

        request(1);
        payload = server.waitForEvent();
        assertEquals(false, payload.event.exchange.getAttachment(DUPLICATE_EVENT_KEY));

        request(0);
        payload = server.waitForEvent();
        assertEquals(false, payload.event.exchange.getAttachment(DUPLICATE_EVENT_KEY));

        request(1);
        payload = server.waitForEvent();
        assertEquals(true, payload.event.exchange.getAttachment(DUPLICATE_EVENT_KEY));

        request(2);
        payload = server.waitForEvent();
        assertEquals(false, payload.event.exchange.getAttachment(DUPLICATE_EVENT_KEY));

        request(1);
        payload = server.waitForEvent();
        assertEquals(false, payload.event.exchange.getAttachment(DUPLICATE_EVENT_KEY));
    }

    private void request(final int which) throws IOException {
        final URL url = new URL(String.format(URL_STRING, server.port) + URL_QUERY_STRINGS[which]);
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        assertEquals(200, conn.getResponseCode());
    }

    @Before
    public void setUp() {
        server = new TestServer("duplicates-test.conf");
    }

    @After
    public void tearDown() {
        server.server.shutdown();
    }
}
