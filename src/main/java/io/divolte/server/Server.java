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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.ConfigFactory;
import io.divolte.server.config.*;
import io.divolte.server.processing.ProcessingPool;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.*;
import io.undertow.server.handlers.cache.DirectBufferCache;
import io.undertow.server.handlers.resource.CachingResourceManager;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.server.handlers.resource.ResourceManager;
import io.undertow.util.Headers;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@ParametersAreNonnullByDefault
public final class Server implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final Undertow undertow;
    private final GracefulShutdownHandler shutdownHandler;

    private final ImmutableMap<String, ProcessingPool<?, AvroRecordBuffer>> sinks;
    private final IncomingRequestProcessingPool incomingRequestProcessingPool;

    private final Optional<String> host;
    private final int port;
    private final long shutdownGracePeriodMills;

    public Server(final ValidatedConfiguration vc) {
        this(vc, (e,b,r) -> {});
    }

    Server(final ValidatedConfiguration vc, final IncomingRequestListener listener) {
        host = vc.configuration().global.server.host;
        port = vc.configuration().global.server.port;
        shutdownGracePeriodMills = vc.configuration().global.server.shutdownGracePeriodMills;

        // First thing we need to do is load all the schemas: the sinks need these, but they come from the
        // mappings.
        final SchemaRegistry schemaRegistry = new SchemaRegistry(vc);

        // Build a set of referenced sinks. These are the only ones we need to instantiate.
        final ImmutableSet<String> referencedSinkNames =
                vc.configuration().mappings.values()
                                           .stream()
                                           .flatMap(mc -> mc.sinks.stream())
                                           .collect(ImmutableSet.toImmutableSet());

        // Instantiate the active sinks:
        //  - As a practical matter, unreferenced sinks have no associated schema, which means they
        //    can't be initialized.
        //  - This is also where we check whether HDFS and Kafka are globally enabled/disabled.
        logger.debug("Initializing active sinks...");
        sinks = vc.configuration().sinks.entrySet()
                  .stream()
                  .filter(sink -> referencedSinkNames.contains(sink.getKey()))
                  .filter(sink -> vc.configuration().global.hdfs.enabled || !(sink.getValue() instanceof HdfsSinkConfiguration))
                  .filter(sink -> vc.configuration().global.gcs.enabled || !(sink.getValue() instanceof GoogleCloudStorageSinkConfiguration))
                  .filter(sink -> vc.configuration().global.kafka.enabled || !(sink.getValue() instanceof KafkaSinkConfiguration))
                  .filter(sink -> vc.configuration().global.gcps.enabled || !(sink.getValue() instanceof GoogleCloudPubSubSinkConfiguration))
                  .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey,
                                                       sink -> sink.getValue()
                                                                   .getFactory()
                                                                   .create(vc, sink.getKey(), schemaRegistry)));
        logger.info("Initialized sinks: {}", sinks.keySet());

        logger.debug("Initializing mappings...");
        incomingRequestProcessingPool = new IncomingRequestProcessingPool(vc, schemaRegistry, sinks, listener);

        logger.debug("Initializing sources...");
        // Now instantiate all the sources. We do this in parallel because instantiation can be quite slow.
        final ImmutableMap<String, HttpSource> sources =
                vc.configuration()
                  .sources
                  .entrySet()
                  .parallelStream()
              .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey,
                                                   source -> source.getValue()
                                                                   .createSource(vc,
                                                                                 source.getKey(),
                                                                                 incomingRequestProcessingPool)));

        logger.debug("Attaching sources: {}", sources.keySet());
        // Once all created we can attach them to the server. This has to be done sequentially.
        PathHandler pathHandler = new PathHandler();
        for (final HttpSource source : sources.values()) {
            pathHandler = source.attachToPathHandler(pathHandler);
        }
        logger.info("Initialized sources: {}", sources.keySet());

        pathHandler.addExactPath("/ping", PingHandler::handlePingRequest);
        if (vc.configuration().global.server.serveStaticResources) {
            // Catch-all handler; must be last if present.
            // XXX: Our static resources assume the default 'browser' endpoint.
            pathHandler.addPrefixPath("/", createStaticResourceHandler());
        }
        final SetHeaderHandler headerHandler =
                new SetHeaderHandler(pathHandler, Headers.SERVER_STRING, "divolte");
        final HttpHandler canonicalPathHandler = new CanonicalPathHandler(headerHandler);
        final GracefulShutdownHandler rootHandler = new GracefulShutdownHandler(
                vc.configuration().global.server.useXForwardedFor ?
                new ProxyAdjacentPeerAddressHandler(canonicalPathHandler) : canonicalPathHandler
                );

        shutdownHandler = rootHandler;
        undertow = Undertow.builder()
                           .addHttpListener(port, host.orElse(null))
                           .setHandler(vc.configuration().global.server.debugRequests
                               ? new RequestDumpingHandler(rootHandler)
                               : rootHandler)
                           .build();
    }

    private static HttpHandler createStaticResourceHandler() {
        final ResourceManager staticResources =
                new ClassPathResourceManager(Server.class.getClassLoader(), "static");
        // Cache tuning is copied from Undertow unit tests.
        final ResourceManager cachedResources =
                new CachingResourceManager(100, 65536,
                                           new DirectBufferCache(1024, 10, 10480),
                                           staticResources,
                                           (int)Duration.ofDays(1).getSeconds());
        final ResourceHandler resourceHandler = new ResourceHandler(cachedResources);
        resourceHandler.setWelcomeFiles("index.html");
        return resourceHandler;
    }

    @Override
    public void run() {
        // When a SIGTERM is received, initialize a graceful shutdown procedure
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        logger.info("Starting server on {}:{}", host.orElse("localhost"), port);
        undertow.start();
    }

    public void shutdown() {

        logger.warn("Requested to kill process, init graceful shutdown");

        try {
            logger.info("Shutting down Undertow, new requests will return SERVICE_UNAVAILABLE 503");
            shutdownHandler.shutdown();

            if(shutdownHandler.awaitShutdown(shutdownGracePeriodMills)) {
                logger.info("Undertow shut down with no remaining connections");
            } else {
                logger.warn("Undertow shut down with remaining connections!");
            }
        } catch (final Exception ie) {
            Thread.currentThread().interrupt();
        } finally {
            undertow.stop();
        }

        logger.info("Stopping thread pools.");

        // Stop the mappings before the sinks to ensure work in progress doesn't get stranded.
        incomingRequestProcessingPool.stop();

        logger.info("Closing the sinks filesystem connection.");
        sinks.values().forEach(ProcessingPool::stop);

        logger.info("Closing HDFS filesystem connection.");
        try {
            FileSystem.closeAll();
        } catch (final IOException ioe) {
            logger.warn("Failed to cleanly close HDFS file system.", ioe);
        }
    }

    public static void main(final String[] args) {
        final ValidatedConfiguration vc = new ValidatedConfiguration(ConfigFactory::load);
        if (!vc.isValid()) {
            vc.errors().forEach(logger::error);
            logger.error("There are configuration errors. Exiting server.");
            System.exit(1);
        }

        // Tell Undertow to use Slf4J for logging by default.
        if (null == System.getProperty("org.jboss.logging.provider")) {
            System.setProperty("org.jboss.logging.provider", "slf4j");
        }
        new Server(vc).run();
    }
}
