/*
 * Copyright 2015 GoDataDriven B.V.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ContainerNode;
import io.divolte.server.ServerTestUtils.TestServer;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

@ParametersAreNonnullByDefault
public class JsonSourceTest {
    private static final String JSON_EVENT_URL_TEMPLATE =
            "http://localhost:%d/json-event?p=0%%3Ai1t84hgy%%3A5AF359Zjq5kUy98u4wQjlIZzWGhN~GlG";
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private Optional<TestServer> testServer = Optional.empty();

    private void startServer(final String configResource) {
        stopServer();
        testServer = Optional.of(new TestServer(configResource));
    }

    private void stopServer() {
        testServer.ifPresent(testServer -> testServer.server.shutdown());
        testServer = Optional.empty();
    }

    @After
    public void tearDown() {
        stopServer();
    }

    private void request() throws IOException {
        request(JSON_MAPPER.createObjectNode());
    }

    private static <T> Consumer<T> noop() {
        return ignored -> {};
    }

    private void request(final ContainerNode json) throws IOException {
        final HttpURLConnection conn = request(json, noop());
        assertEquals(204, conn.getResponseCode());
    }

    private HttpURLConnection startRequest() throws IOException {
        final URL url = new URL(String.format(JSON_EVENT_URL_TEMPLATE, testServer.get().port));
        return (HttpURLConnection) url.openConnection();
    }

    private HttpURLConnection request(final ContainerNode json,
                                      final Consumer<HttpURLConnection> preRequest) throws IOException {
        final HttpURLConnection conn = startRequest();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        preRequest.accept(conn);
        try (final OutputStream requestBody = conn.getOutputStream()) {
            JSON_MAPPER.writeValue(requestBody, json);
        }
        // This is the canonical way to wait for the server to respond.
        conn.getResponseCode();
        return conn;
    }

    @Test
    public void shouldSupportMobileSource() {
        startServer("mobile-source.conf");
    }

    @Test
    public void shouldSupportPostingJsonToEndpoint() throws IOException {
        startServer("mobile-source.conf");
        request();
    }

    @Test
    public void shouldOnlySupportPostRequests() throws IOException {
        startServer("mobile-source.conf");
        final HttpURLConnection conn = startRequest();
        conn.setRequestMethod("GET");

        // 405: Method Not Allowed
        assertEquals(405, conn.getResponseCode());
        assertEquals("POST", conn.getHeaderField("Allow"));
    }

    @Test
    @Ignore("Not yet supported")
    public void shouldOnlySupportJsonRequests() throws IOException {
        startServer("mobile-source.conf");
        final HttpURLConnection conn = startRequest();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
        conn.setDoOutput(true);
        try (final OutputStream requestBody = conn.getOutputStream()) {
            requestBody.write("This is not a JSON body.".getBytes(StandardCharsets.UTF_8));
        }

        // 415: Unsupported Media Type
        assertEquals(415, conn.getResponseCode());
    }
}
