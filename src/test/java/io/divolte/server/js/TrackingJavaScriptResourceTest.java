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

package io.divolte.server.js;

import io.divolte.server.ValidatedConfiguration;
import io.undertow.util.ETag;

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
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);
        trackingJavaScript = new TrackingJavaScriptResource(vc);
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
        final ETag eTag = trackingJavaScript.getEntityBody().getETag();
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
        final ETag eTag = gzippedBody.get().getETag();
        validateEtag(eTag);
        assertThat(eTag, is(not(equalTo(trackingJavaScript.getEntityBody().getETag()))));
    }

    private static void validateEntityBody(final ByteBuffer entityBody) {
        assertThat(entityBody, is(notNullValue()));
        assertThat(entityBody.isReadOnly(), is(true));
        assertThat(entityBody.remaining(), is(greaterThan(0)));
    }

    private static void validateEtag(final ETag eTag) {
        assertThat(eTag, is(notNullValue()));
        assertThat(eTag.isWeak(), is(false));
        assertThat(eTag.getTag(), not(isEmptyOrNullString()));
        assertThat(eTag.toString(), startsWith("\""));
        assertThat(eTag.toString(), endsWith("\""));
        assertThat(eTag.toString().length(), is(greaterThan(2)));
    }
}
