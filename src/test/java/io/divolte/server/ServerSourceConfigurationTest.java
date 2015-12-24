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

import io.divolte.server.ServerTestUtils.TestServer;
import org.junit.After;
import org.junit.Test;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

@ParametersAreNonnullByDefault
public class ServerSourceConfigurationTest {

    private static final String BROWSER_EVENT_URL_TEMPLATE =
        "http://localhost:%d%s/csc-event?"
            + "p=0%%3Ai1t84hgy%%3A5AF359Zjq5kUy98u4wQjlIZzWGhN~GlG&"
            + "s=0%%3Ai1t84hgy%%3A95CbiPCYln_1e0a6rFvuRkDkeNnc6KC8&"
            + "v=0%%3A1fF6GFGjDOQiEx_OxnTm_tl4BH91eGLF&"
            + "e=0%%3A1fF6GFGjDOQiEx_OxnTm_tl4BH91eGLF0&"
            + "c=i1t8q2b6&"
            + "n=f&"
            + "f=f&"
            + "l=http%%3A%%2F%%2Flocalhost%%3A8290%%2F&"
            + "i=1ak&"
            + "j=sj&"
            + "k=2&"
            + "w=uq&"
            + "h=qd&"
            + "t=pageView&"
            + "x=si9804";

    private Optional<TestServer> testServer = Optional.empty();

    private void startServer(final Optional<String> configResource) {
        stopServer();
        final TestServer newServer = configResource.map(TestServer::new).orElseGet(TestServer::new);
        testServer = Optional.of(newServer);
    }

    public void stopServer() {
        testServer.ifPresent(testServer -> testServer.server.shutdown());
        testServer = Optional.empty();
    }

    private void request(final String sourcePrefix) throws IOException {
        request(sourcePrefix, 200);
    }

    private void request(final String sourcePrefix, final int expectedResponseCode) throws IOException {
        final URL url = new URL(String.format(BROWSER_EVENT_URL_TEMPLATE,
                                              testServer.get().port,
                                              sourcePrefix));
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        assertEquals(expectedResponseCode, conn.getResponseCode());
    }

    @Test
    public void shouldRegisterDefaultBrowserSource() throws IOException, InterruptedException {
        // Test the default browser source that should be present by default.
        startServer(Optional.empty());
        request("");
        testServer.get().waitForEvent();
    }

    @Test
    public void shouldRegisterExplicitSourceOnly() throws IOException, InterruptedException {
        // Test that if an explicit source is supplied, the builtin defaults are not present.
        startServer(Optional.of("browser-source-explicit.conf"));
        request("/a-prefix");
        testServer.get().waitForEvent();
        request("", 404);
    }

    @Test
    public void shouldSupportLongPaths() throws IOException, InterruptedException {
        // Test that the browser sources work with different types of path.
        startServer(Optional.of("browser-source-long-prefix.conf"));
        request("/a/multi/component/prefix");
        testServer.get().waitForEvent();
    }

    @Test
    public void shouldSupportMultipleBrowserSources() throws IOException, InterruptedException {
        // Test that multiple browser sources are supported.
        startServer(Optional.of("browser-source-multiple.conf"));
        request("/path1");
        request("/path2");
        testServer.get().waitForEvent();
        testServer.get().waitForEvent();
    }

    @After
    public void tearDown() {
        stopServer();
    }
}
