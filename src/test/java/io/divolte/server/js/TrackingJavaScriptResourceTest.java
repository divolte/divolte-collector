/*
 * Copyright 2016 GoDataDriven B.V.
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

import com.google.common.collect.ImmutableList;
import com.typesafe.config.ConfigFactory;
import io.divolte.server.config.ValidatedConfiguration;
import io.undertow.util.ETag;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.Config;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.ParametersAreNonnullByDefault;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
@ParametersAreNonnullByDefault
public class TrackingJavaScriptResourceTest {

    @Parameterized.Parameters(name = "{index}: {0} configuration")
    public static Iterable<Object[]> configurations() {
        return ImmutableList.of(
            new Object[] { "default",          ConfigFactory.load() },
            new Object[] { "non-default-name", ConfigFactory.load("browser-source-custom-javascript-name.conf")},
            new Object[] { "logging-enabled",  ConfigFactory.load("browser-source-javascript-logging.conf")},
            new Object[] { "verbose-enabled",  ConfigFactory.load("browser-source-javascript-debugging.conf")}
        );
    }

    private final Config config;

    private TrackingJavaScriptResource trackingJavaScript;

    public TrackingJavaScriptResourceTest(final String name, final Config config) {
        this.config = Objects.requireNonNull(config);
    }

    @Before
    public void setup() throws IOException {
        // Essential test to ensure at build-time that our JavaScript can be compiled.
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);
        trackingJavaScript = TrackingJavaScriptResource.create(vc, "browser");
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
