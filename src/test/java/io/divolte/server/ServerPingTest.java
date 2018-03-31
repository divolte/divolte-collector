/*
 * Copyright 2018 GoDataDriven B.V.
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
import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import io.divolte.server.ServerTestUtils.TestServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@ParametersAreNonnullByDefault
public class ServerPingTest {

    @Nullable
    private TestServer testServer;

    @Before
    public void setup() {
        testServer = new TestServer("reference-test-shutdown.conf");
    }

    @Test
    public void shouldRespondToHealthCheck() throws IOException, InterruptedException {
        Preconditions.checkState(null != testServer);
        final URL url = new URL(String.format("http://%s:%d/ping", testServer.host, testServer.port));

        // Check the server is up.
        pingServer(url, Optional.of("pong"));

        // Start the shutdown procedure.
        testServer.shutdown();

        // The shutdown is async, and we're racing it here.
        // As a result we need to poll until we see the 504. Any 200 codes in the interim can be ignored.
        final Stopwatch timer = Stopwatch.createStarted();
        final Duration timeout = Duration.ofSeconds(10);
        loop:
        for (;;) {
            final int responseCode = pingServer(url, Optional.empty());
            switch (responseCode) {
                case HTTP_OK:
                    // Not yet failure; check to see if too much time has elapsed.
                    if (timer.elapsed().compareTo(timeout) >= 0) {
                        // Took too long. Fail the test.
                        fail(String.format("Server took too long to start shutting down; expected %d but after %s server ping was still responding with %d.",
                                           HTTP_UNAVAILABLE, timeout, HTTP_OK));
                    }
                    // Wait a bit before trying again.
                    Thread.sleep(Duration.ofMillis(100).toMillis());
                    break;
                case HTTP_UNAVAILABLE:
                    // Test success.
                    break loop;
                default:
                    fail(String.format("Unexpected response code from server: %d; expected %d or %d.",
                        responseCode, HTTP_OK, HTTP_UNAVAILABLE));
            }
        }
    }

    private static int pingServer(final URL url, final Optional<String> expectedBody) throws IOException {
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        final int responseCode;
        try {
            conn.setRequestMethod("GET");
            responseCode = conn.getResponseCode();
            assertEquals("text/plain; charset=utf-8", conn.getContentType());
            expectedBody.ifPresent(expected -> {
                final String body =
                    new String(IOExceptions.wrap(() -> ByteStreams.toByteArray(conn.getInputStream())).get(),
                               StandardCharsets.UTF_8);
                assertEquals(expected, body);
            });
        } finally {
            conn.disconnect();
        }
        return responseCode;
    }

    @After
    public void tearDown() {
        if (null != testServer) {
            testServer = null;
        }
    }
}
