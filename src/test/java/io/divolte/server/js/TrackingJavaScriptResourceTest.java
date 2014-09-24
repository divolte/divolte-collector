package io.divolte.server.js;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

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
        final ByteBuffer entityBody = trackingJavaScript.getEntityBody().getBody();
        validateEntityBody(entityBody);
    }

    @Test
    public void testETagIsValid() throws IOException {
        final String eTag = trackingJavaScript.getEntityBody().getETag();
        validateEtag(eTag);
    }

    @Test
    public void testResourceNameIsPresent() throws IOException {
        final String resourceName = trackingJavaScript.getResourceName();
        assertThat(resourceName, not(isEmptyOrNullString()));
    }

    @Test
    public void testGzippedJavascriptAvailable() throws IOException {
        final Optional<HttpBody> gzippedBody = trackingJavaScript.getEntityBody().getGzippedBody();
        assertThat(gzippedBody.isPresent(), is(true));
        final ByteBuffer gzippedData = gzippedBody.get().getBody();
        validateEntityBody(gzippedData);
        assertThat(gzippedData.remaining(),
                   is(lessThan(trackingJavaScript.getEntityBody().getBody().remaining())));
    }

    @Test
    public void testGzippedETagIsValid() {
        final Optional<HttpBody> gzippedBody = trackingJavaScript.getEntityBody().getGzippedBody();
        assertThat(gzippedBody.isPresent(), is(true));
        final String eTag = gzippedBody.get().getETag();
        validateEtag(eTag);
        assertThat(eTag, is(not(equalTo(trackingJavaScript.getEntityBody().getETag()))));
    }

    private static void validateEntityBody(final ByteBuffer entityBody) {
        assertThat(entityBody, is(notNullValue()));
        assertThat(entityBody.isReadOnly(), is(true));
        assertThat(entityBody.remaining(), is(greaterThan(0)));
    }

    private static void validateEtag(final String eTag) {
        assertThat(eTag, is(notNullValue()));
        assertThat(eTag, startsWith("\""));
        assertThat(eTag, endsWith("\""));
        assertThat(eTag.length(), is(greaterThan(2)));
    }
}
