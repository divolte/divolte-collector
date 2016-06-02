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
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.divolte.server.ServerTestUtils.TestServer;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;

import static java.net.HttpURLConnection.*;
import static org.junit.Assert.assertEquals;

@ParametersAreNonnullByDefault
public class JsonSourceTest {
    private static final String JSON_EVENT_WITHOUT_PARTYID_URL_TEMPLATE = "http://localhost:%d/json-event";
    private static final String JSON_EVENT_URL_TEMPLATE = JSON_EVENT_WITHOUT_PARTYID_URL_TEMPLATE + "?p=0%%3Ai1t84hgy%%3A5AF359Zjq5kUy98u4wQjlIZzWGhN~GlG";
    private static final String JSON_EVENT_WITH_BROKEN_PARTYID_URL_TEMPLATE = JSON_EVENT_WITHOUT_PARTYID_URL_TEMPLATE + "?p=notavalidpartyid";
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static final ObjectNode EMPTY_JSON_OBJECT = JSON_MAPPER.createObjectNode();

    private Optional<TestServer> testServer = Optional.empty();
    private Optional<String> urlTemplate = Optional.empty();

    private void startServer(final String configResource) {
        stopServer();
        testServer = Optional.of(new TestServer(configResource));
        urlTemplate = Optional.of(JSON_EVENT_URL_TEMPLATE);
    }

    private void stopServer() {
        testServer.ifPresent(testServer -> testServer.server.shutdown());
        testServer = Optional.empty();
        urlTemplate = Optional.empty();
    }

    @After
    public void tearDown() {
        stopServer();
    }

    private HttpURLConnection request() throws IOException {
        return request(EMPTY_JSON_OBJECT);
    }

    private static <T> Consumer<T> noop() {
        return ignored -> {};
    }

    private HttpURLConnection request(final ContainerNode json) throws IOException {
        return request(json, noop());
    }

    private HttpURLConnection request(final Consumer<HttpURLConnection> preRequest) throws IOException {
        return request(JSON_MAPPER.createObjectNode(), preRequest);
    }

    private HttpURLConnection startRequest() throws IOException {
        final String url = String.format(urlTemplate.orElseThrow(() -> new IllegalStateException("No URL template available")),
                                         testServer.orElseThrow(() -> new IllegalStateException("No test server available")).port);
        return (HttpURLConnection) new URL(url).openConnection();
    }

    private HttpURLConnection startJsonPostRequest() throws IOException {
        final HttpURLConnection conn = startRequest();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        return conn;
    }

    private HttpURLConnection request(final ContainerNode json,
                                      final Consumer<HttpURLConnection> preRequest) throws IOException {
        final HttpURLConnection conn = startJsonPostRequest();
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
        startServer("json-source.conf");
    }

    @Test
    public void shouldSupportPostingJsonToEndpoint() throws IOException {
        startServer("json-source.conf");
        final HttpURLConnection conn = request();

        assertEquals(HTTP_NO_CONTENT, conn.getResponseCode());
    }

    @Test
    public void shouldOnlySupportPostRequests() throws IOException {
        startServer("json-source.conf");
        final HttpURLConnection conn = startRequest();
        conn.setRequestMethod("GET");

        assertEquals(HTTP_BAD_METHOD, conn.getResponseCode());
        assertEquals("POST", conn.getHeaderField("Allow"));
    }

    @Test
    public void shouldOnlySupportJsonRequests() throws IOException {
        startServer("json-source.conf");
        final HttpURLConnection conn = startRequest();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
        conn.setDoOutput(true);
        try (final OutputStream requestBody = conn.getOutputStream()) {
            requestBody.write("This is not a JSON body.".getBytes(StandardCharsets.UTF_8));
        }

        assertEquals(HTTP_UNSUPPORTED_TYPE, conn.getResponseCode());
    }

    @Test
    public void shouldRejectEmptyRequests() throws IOException {
        startServer("json-source.conf");
        final HttpURLConnection conn = startJsonPostRequest();

        assertEquals(HTTP_BAD_REQUEST, conn.getResponseCode());
    }

    @Test
    public void shouldRejectRequestsWithoutPartyId() throws IOException {
        startServer("json-source.conf");
        urlTemplate = Optional.of(JSON_EVENT_WITHOUT_PARTYID_URL_TEMPLATE);
        final HttpURLConnection conn = request();

        assertEquals(HTTP_BAD_REQUEST, conn.getResponseCode());
    }

    @Test
    public void shouldRejectRequestsWithBrokenPartyId() throws IOException {
        startServer("json-source.conf");
        urlTemplate = Optional.of(JSON_EVENT_WITH_BROKEN_PARTYID_URL_TEMPLATE);
        final HttpURLConnection conn = request();

        assertEquals(HTTP_BAD_REQUEST, conn.getResponseCode());
    }

    @Test
    public void shouldAcceptRequestsWithoutContentLength() throws IOException {
        startServer("json-source.conf");

        final HttpURLConnection conn = request(c -> {
            // Chunked-streaming mode disables buffering and the content-length header.
            c.setChunkedStreamingMode(1);
        });

        assertEquals(HTTP_NO_CONTENT, conn.getResponseCode());
    }

    @Test
    public void shouldRejectRequestsWithBodyLessThanContentLength() throws IOException {
        startServer("json-source.conf");

        final HttpURLConnection conn = startJsonPostRequest();
        final byte[] bodyBytes = JSON_MAPPER.writeValueAsBytes(EMPTY_JSON_OBJECT);
        // Explicitly lie about the length of the content we're sending.
        conn.setChunkedStreamingMode(1);
        conn.setRequestProperty("content-length", String.valueOf(bodyBytes.length + 1));
        // Sanity check that the underlying implementation didn't discard our content length.
        Assume.assumeTrue("Cannot manipulate content-length headers; try setting 'sun.net.http.allowRestrictedHeaders'.",
                          null != conn.getRequestProperty("content-length"));
        try (final OutputStream requestBody = conn.getOutputStream()) {
            requestBody.write(bodyBytes);
        }

        assertEquals(HTTP_BAD_REQUEST, conn.getResponseCode());
    }

    @Test
    public void shouldRejectRequestsWithBodyMoreThanContentLength() throws IOException {
        startServer("json-source.conf");

        final HttpURLConnection conn = startJsonPostRequest();
        final byte[] bodyBytes = JSON_MAPPER.writeValueAsBytes(EMPTY_JSON_OBJECT);
        // Explicitly lie about the length of the content we're sending.
        conn.setChunkedStreamingMode(1);
        conn.setRequestProperty("content-length", String.valueOf(bodyBytes.length - 1));
        // Sanity check that the underlying implementation didn't discard our content length.
        Assume.assumeTrue("Cannot manipulate content-length headers; try setting 'sun.net.http.allowRestrictedHeaders'.",
                null != conn.getRequestProperty("content-length"));
        try (final OutputStream requestBody = conn.getOutputStream()) {
            requestBody.write(bodyBytes);
        }

        assertEquals(HTTP_BAD_REQUEST, conn.getResponseCode());
    }

    private static ObjectNode buildBigJsonPayload(final int approximateSize) {
        final ObjectNode root = JSON_MAPPER.createObjectNode();
        final char[] propertyValue = new char[approximateSize];
        Arrays.fill(propertyValue, 'X');
        root.put("p", new String(propertyValue));
        return root;
    }

    @Test
    public void shouldRejectRequestsWithTooLargeContentLength() throws IOException {
        startServer("json-source.conf");

        final HttpURLConnection conn = startJsonPostRequest();
        final byte[] bodyBytes = JSON_MAPPER.writeValueAsBytes(buildBigJsonPayload(100000));
        // Here we're explicitly declaring that we're going to be large.
        conn.setFixedLengthStreamingMode(bodyBytes.length);
        try (final OutputStream requestBody = conn.getOutputStream()) {
            requestBody.write(bodyBytes);
        }

        assertEquals(HTTP_ENTITY_TOO_LARGE, conn.getResponseCode());
    }

    @Test
    public void shouldRejectRequestsWithTooLargeBody() throws IOException {
        startServer("json-source.conf");

        final HttpURLConnection conn = startJsonPostRequest();
        // Here we're not declaring ahead of time that we will be too large. But the body will be.
        conn.setChunkedStreamingMode(128);
        try (final OutputStream requestBody = conn.getOutputStream()) {
            JSON_MAPPER.writeValue(requestBody, buildBigJsonPayload(100000));
        }

        assertEquals(HTTP_ENTITY_TOO_LARGE, conn.getResponseCode());
    }

    @Test
    public void shouldAcceptBodyLargerThanOneChunk() throws IOException {
        startServer("json-source.conf");

        // Assumes the configuration has a max body size greater than 2 chunks.
        // (Disable the content-length header to force chunked reading.)
        final HttpURLConnection conn = request(buildBigJsonPayload(ChunkyByteBuffer.CHUNK_SIZE * 2),
                                               c -> c.setChunkedStreamingMode(8));

        assertEquals(HTTP_NO_CONTENT, conn.getResponseCode());
    }
}
