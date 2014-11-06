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
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ServerSideCookieEventHandlerTest {
    private TestServer server;

    @Test
    public void shouldRegisterServerSideCookieEvent() throws IOException, RuntimeException, InterruptedException {
        final URL url = new URL(String.format("http://localhost:%d/ssc-event?v=supercalifragilisticexpialidocious&t=myEventType", server.port));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        assertEquals(202, conn.getResponseCode());

        EventPayload event = server.waitForEvent();
        assertEquals("myEventType", event.record.get("eventType"));
        assertEquals("supercalifragilisticexpialidocious", event.record.get("pageViewId"));
    }

    @Before
    public void setUp() {
        server = new TestServer("server-side-cookies-test.conf");
        server.server.run();
    }

    @After
    public void tearDown() {
        server.server.shutdown();
    }
}
