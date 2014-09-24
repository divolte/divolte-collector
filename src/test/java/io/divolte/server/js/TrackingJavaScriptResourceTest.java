package io.divolte.server.js;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class TrackingJavaScriptResourceTest {

    private final Config config = ConfigFactory.load();

    private TrackingJavaScriptResource trackingJavaScript;

    @Before
    public void setup() throws IOException {
        // Essential test to ensure at build-time that our JavaScript can be compiled.
        trackingJavaScript = new TrackingJavaScriptResource(config);
    }

    @After
    public void teardown() {
        trackingJavaScript = null;
    }

    @Test
    public void testJavascriptAvailable() throws IOException {
        final ByteBuffer entityBody = trackingJavaScript.getEntityBody();
        assertThat(entityBody, is(notNullValue()));
        assertThat(entityBody.isReadOnly(), is(true));
        assertThat(entityBody.remaining(), is(greaterThan(0)));
    }

    @Test
    public void testETagIsValid() throws IOException {
        final String eTag = trackingJavaScript.getETag();
        assertThat(eTag, is(notNullValue()));
        assertThat(eTag, startsWith("\""));
        assertThat(eTag, endsWith("\""));
        assertThat(eTag.length(), is(greaterThan(2)));
    }

    @Test
    public void testResourceNameIsPresent() throws IOException {
        final String resourceName = trackingJavaScript.getResourceName();
        assertThat(resourceName, not(isEmptyOrNullString()));
    }
}
