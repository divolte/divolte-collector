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

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.divolte.server.config.ValidatedConfiguration;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public final class ServerTestUtils {
    /*
     * Theoretically, this is prone to race conditions,
     * but in practice, it should be fine due to the way
     * TCP stacks allocate port numbers (i.e. increment
     * for the next one).
     */
    public static int findFreePort() {
        try (final ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (final IOException e) {
            return -1;
        }
    }

    private static Config REFERENCE_TEST_CONFIG = ConfigFactory.parseResources("reference-test.conf");

    @ParametersAreNonnullByDefault
    public static final class EventPayload {
        final DivolteEvent event;
        final AvroRecordBuffer buffer;
        final GenericRecord record;

        public EventPayload(final DivolteEvent event,
                             final AvroRecordBuffer buffer,
                             final GenericRecord record) {
            this.event = Objects.requireNonNull(event);
            this.buffer = Objects.requireNonNull(buffer);
            this.record = Objects.requireNonNull(record);
        }
    }

    @ParametersAreNonnullByDefault
    public static final class TestServer {
        final Config config;
        final String host;
        final int port;
        final Server server;
        final BlockingQueue<EventPayload> events;

        public TestServer() {
            this(findFreePort(), REFERENCE_TEST_CONFIG);
        }

        public TestServer(final String configResource) {
            this(findFreePort(),
                 ConfigFactory.parseResources(configResource)
                              .withFallback(REFERENCE_TEST_CONFIG));
        }

        public TestServer(final String configResource, final Map<String,Object> extraConfig) {
            this(findFreePort(),
                 ConfigFactory.parseMap(extraConfig, "Test-specific overrides")
                              .withFallback(ConfigFactory.parseResources(configResource))
                              .withFallback(REFERENCE_TEST_CONFIG));
        }

        private TestServer(final int port, final Config config) {
            this.port = port;
            this.host = getBindAddress(config);
            this.config = config.withValue("divolte.global.server.host", ConfigValueFactory.fromAnyRef(host))
                                .withValue("divolte.global.server.port", ConfigValueFactory.fromAnyRef(port));

            events = new ArrayBlockingQueue<>(100);
            final ValidatedConfiguration vc = new ValidatedConfiguration(() -> this.config);
            Preconditions.checkArgument(vc.isValid(),
                                        "Invalid test server configuration: %s", vc.errors());
            server = new Server(vc, (event, buffer, record) -> events.add(new EventPayload(event, buffer, record)));
            server.run();
        }

        private static String getBindAddress(final Config config) {
            final String bindAddress;
            System.err.println("Hmm: " + System.getProperty("io.divolte.test.bindExternal"));
            if (Boolean.getBoolean("io.divolte.test.bindExternal")) {
                try {
                    bindAddress = InetAddress.getLocalHost().getHostAddress();
                } catch (final UnknownHostException e) {
                    throw new RuntimeException("Unable to determine external IP address", e);
                }
            } else {
                bindAddress = "127.0.0.1";
            }
            System.err.println("Binding to IP address: " + bindAddress);
            return bindAddress;
        }

        static TestServer createTestServerWithDefaultNonTestConfiguration() {
            return new TestServer(findFreePort(), ConfigFactory.defaultReference());
        }

        public EventPayload waitForEvent() throws InterruptedException {
            // SauceLabs can take quite a while to fire up everything.
            return Optional.ofNullable(events.poll(10, TimeUnit.SECONDS)).orElseThrow(() -> new RuntimeException("Timed out while waiting for server side event to occur."));
        }

        public boolean eventsRemaining() {
            return !events.isEmpty();
        }
    }
}
