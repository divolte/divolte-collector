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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.divolte.server.config.ValidatedConfiguration;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class ServerTestUtils {
    private static final Logger logger = LoggerFactory.getLogger(ServerTestUtils.class);

    /*
     * List of ports to cycle through.
     *
     * We use this list instead of a random OS-assigned port because
     * some environments (e.g. Sauce Labs) only support specific ports
     * when connecting to 'localhost'.
     * See: https://wiki.saucelabs.com/display/DOCS/Sauce+Connect+Proxy+FAQS#SauceConnectProxyFAQS-CanIAccessApplicationsonlocalhost?
     *
     * Note: Browsers often block ports on localhost.
     * https://fetch.spec.whatwg.org/#port-blocking
     * (Safari and Firefox use this list; not sure about the rest.)
     * Note: Android can't use 5555 or 8080 with Sauce Connect.
     */
    private static int[] SAFE_PORTS = {
        2000, 2001, 2020, 2109, 2222, 2310,
        3000, 3001, 3030, 3210, 3333,
        4000, 4001, 4040, 4321, 4502, 4503, 4567,
        5000, 5001, 5050, 5432,
        6001, 6060, 6543,
        7000, 7070, 7774, 7777,
        8000, 8001, 8003, 8031, 8081, 8765, 8777, 8888,
        9000, 9001, 9080, 9090, 9876, 9877, 9999,
        49221,
        55001,
    };
    private static AtomicInteger nextSafePortIndex = new AtomicInteger(0);
    private static int findFreePort(final InetAddress bindAddress) {
        for(int attempt = 0; attempt < SAFE_PORTS.length * 2; ++attempt) {
            final int candidatePortIndex = nextSafePortIndex.getAndUpdate((lastPort) -> (lastPort + 1) % SAFE_PORTS.length);
            final int candidatePort = SAFE_PORTS[candidatePortIndex];
            try (final ServerSocket socket = new ServerSocket(candidatePort, 1, bindAddress)) {
                return socket.getLocalPort();
            } catch (final IOException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Could not bind port %d on %s; assuming already in use.",
                                               candidatePort, bindAddress), e);
                }
            }
        }
        // Give up if we go through the list twice.
        throw new RuntimeException("Could not find unused safe port.");
    }

    private static InetAddress getBindAddress() {
        final InetAddress bindAddress;
        if (Boolean.getBoolean("io.divolte.test.bindExternal")) {
            try {
                bindAddress = InetAddress.getLocalHost();
            } catch (final UnknownHostException e) {
                throw new UncheckedIOException("Unable to determine external IP address", e);
            }
        } else {
            bindAddress = InetAddress.getLoopbackAddress();
        }
        return bindAddress;
    }

    private static InetSocketAddress findBindPort() {
        final InetAddress bindAddress = getBindAddress();
        return new InetSocketAddress(bindAddress, findFreePort(bindAddress));
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
        private final Server server;
        final BlockingQueue<EventPayload> events;

        public TestServer() {
            this(findBindPort(), REFERENCE_TEST_CONFIG);
        }

        public TestServer(final String configResource) {
            this(findBindPort(),
                 ConfigFactory.parseResources(configResource)
                              .withFallback(REFERENCE_TEST_CONFIG));
        }

        public TestServer(final String configResource, final Map<String,Object> extraConfig) {
            this(findBindPort(),
                 ConfigFactory.parseMap(extraConfig, "Test-specific overrides")
                              .withFallback(ConfigFactory.parseResources(configResource))
                              .withFallback(REFERENCE_TEST_CONFIG));
        }

        private TestServer(final InetSocketAddress hostPort, final Config config) {
            this.port = hostPort.getPort();
            this.host = hostPort.getAddress().getHostAddress();
            this.config = config.withValue("divolte.global.server.host", ConfigValueFactory.fromAnyRef(host))
                                .withValue("divolte.global.server.port", ConfigValueFactory.fromAnyRef(port));

            events = new ArrayBlockingQueue<>(100);
            final ValidatedConfiguration vc = new ValidatedConfiguration(() -> this.config);
            Preconditions.checkArgument(vc.isValid(),
                                        "Invalid test server configuration: %s", vc.errors());
            server = new Server(vc, (event, buffer, record) -> events.add(new EventPayload(event, buffer, record)));
            try {
                server.run();
            } catch (final RuntimeException e) {
                throw new RuntimeException("Error starting test server listening on: " + hostPort, e);
            }
        }

        static TestServer createTestServerWithDefaultNonTestConfiguration() {
            return new TestServer(findBindPort(), ConfigFactory.defaultReference());
        }

        public EventPayload waitForEvent() throws InterruptedException {
            // SauceLabs can take quite a while to fire up everything.
            return waitForEvent(10, TimeUnit.SECONDS);
        }

        public EventPayload waitForEvent(final long timeout, final TimeUnit unit) throws InterruptedException {
            return Optional.ofNullable(events.poll(timeout, unit))
                           .orElseThrow(() -> new RuntimeException("Timed out while waiting for server side event to occur."));
        }

        public boolean eventsRemaining() {
            return !events.isEmpty();
        }

        public void shutdown() {
            shutdown(false);
        }

        public void shutdown(final boolean waitForShutdown) {
            // The server can take a little while to shut down, so we do this asynchronously if possible.
            // (This is harmless: new servers will listen on a different port.)
            if (waitForShutdown) {
                server.shutdown();
            } else {
                new Thread(server::shutdown).start();
            }
        }
    }
}
