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

import com.typesafe.config.ConfigValueFactory;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.avro.generic.GenericRecord;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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

    @ParametersAreNonnullByDefault
    public static final class EventPayload {
        final HttpServerExchange exchange;
        final AvroRecordBuffer buffer;
        final GenericRecord record;

        public EventPayload(final HttpServerExchange exchange,
                             final AvroRecordBuffer buffer,
                             final GenericRecord record) {
            this.exchange = Objects.requireNonNull(exchange);
            this.buffer = Objects.requireNonNull(buffer);
            this.record = Objects.requireNonNull(record);
        }
    }

    @ParametersAreNonnullByDefault
    public static final class TestServer {
        final Config config;
        final int port;
        final Server server;
        final BlockingQueue<EventPayload> events;

        public TestServer(final String configResource) {
            events = new ArrayBlockingQueue<>(100);
            port = findFreePort();
            config = ConfigFactory.parseResources(configResource)
                        .withFallback(ConfigFactory.parseResources("reference-test.conf"))
                        .withValue("divolte.server.port", ConfigValueFactory.fromAnyRef(port));
            server = new Server(config, (exchange, buffer, record) -> events.add(new EventPayload(exchange, buffer, record)));
        }

        public EventPayload waitForEvent() throws InterruptedException {
            // SauceLabs can take quite a while to fire up everything.
            return Optional.ofNullable(events.poll(40, TimeUnit.SECONDS)).orElseThrow(() -> new RuntimeException("Timed out while waiting for server side event to occur."));
        }
    }
}
