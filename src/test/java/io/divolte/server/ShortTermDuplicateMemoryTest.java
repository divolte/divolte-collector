package io.divolte.server;

import static io.divolte.server.IncomingRequestProcessor.*;
import static org.junit.Assert.*;
import io.divolte.server.ServerTestUtils.EventPayload;
import io.divolte.server.ServerTestUtils.TestServer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
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
        EventPayload event;

        request(0);
        event = server.waitForEvent();
        assertEquals(false, event.exchange.getAttachment(DUPLICATE_EVENT_KEY));

        request(1);
        event = server.waitForEvent();
        assertEquals(false, event.exchange.getAttachment(DUPLICATE_EVENT_KEY));

        request(0);
        event = server.waitForEvent();
        assertEquals(true, event.exchange.getAttachment(DUPLICATE_EVENT_KEY));
    }

    @Test
    public void shouldForgetAboutRequestHashesAfterSomeTime() throws IOException, InterruptedException {
        EventPayload event;

        request(1);
        event = server.waitForEvent();
        assertEquals(false, event.exchange.getAttachment(DUPLICATE_EVENT_KEY));

        request(0);
        event = server.waitForEvent();
        assertEquals(false, event.exchange.getAttachment(DUPLICATE_EVENT_KEY));

        request(1);
        event = server.waitForEvent();
        assertEquals(true, event.exchange.getAttachment(DUPLICATE_EVENT_KEY));

        request(2);
        event = server.waitForEvent();
        assertEquals(false, event.exchange.getAttachment(DUPLICATE_EVENT_KEY));

        request(1);
        event = server.waitForEvent();
        assertEquals(false, event.exchange.getAttachment(DUPLICATE_EVENT_KEY));
    }

    private void request(int which) throws MalformedURLException, IOException, ProtocolException {
        URL url = new URL(String.format(URL_STRING, server.port) + URL_QUERY_STRINGS[which]);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        assertEquals(200, conn.getResponseCode());
    }

    @Before
    public void setUp() {
        server = new TestServer("duplicates-test.conf");
        server.server.run();
    }

    @After
    public void tearDown() {
        server.server.shutdown();
    }
}
