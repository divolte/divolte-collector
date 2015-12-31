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

import io.divolte.server.ServerTestUtils.TestServer;
import org.junit.After;
import org.junit.Test;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class MobileSourceTest {
    private Optional<TestServer> testServer = Optional.empty();

    private void startServer(final String configResource) {
        stopServer();
        testServer = Optional.of(new TestServer(configResource));
    }

    public void stopServer() {
        testServer.ifPresent(testServer -> testServer.server.shutdown());
        testServer = Optional.empty();
    }

    @Test
    public void shouldSupportMobileSource() {
        startServer("mobile-source.conf");
    }

    @After
    public void tearDown() {
        stopServer();
    }
}
