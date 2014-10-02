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
