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

import io.divolte.server.js.TrackingJavaScriptResource;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.CanonicalPathHandler;
import io.undertow.server.handlers.GracefulShutdownHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.SetHeaderHandler;
import io.undertow.server.handlers.cache.DirectBufferCache;
import io.undertow.server.handlers.resource.CachingResourceManager;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.server.handlers.resource.ResourceManager;
import io.undertow.util.Headers;
import io.undertow.util.Methods;

import java.io.IOException;
import java.time.Duration;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

@ParametersAreNonnullByDefault
public final class Server implements Runnable {
    private static final long HTTP_SHUTDOWN_GRACE_PERIOD_MILLIS = 120000L;
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final Undertow undertow;
    private final GracefulShutdownHandler shutdownHandler;

    private final IncomingRequestProcessingPool processingPool;

    private final String host;
    private final int port;

    public Server(final ValidatedConfiguration vc) {
        this(vc, (e,b,r) -> {});
    }

    Server(final ValidatedConfiguration vc, IncomingRequestListener listener) {
        host = vc.configuration().server.host;
        port = vc.configuration().server.port;

        processingPool = new IncomingRequestProcessingPool(vc, listener);
        final ClientSideCookieEventHandler clientSideCookieEventHandler =
                new ClientSideCookieEventHandler(processingPool);
        final TrackingJavaScriptResource trackingJavaScript = loadTrackingJavaScript(vc);
        final HttpHandler javascriptHandler = new AllowedMethodsHandler(new JavaScriptHandler(trackingJavaScript), Methods.GET);

        final PathHandler handler = new PathHandler();
        handler.addExactPath("/csc-event",
                             new AllowedMethodsHandler(clientSideCookieEventHandler, Methods.GET));
        handler.addExactPath('/' + trackingJavaScript.getScriptName(), javascriptHandler);
        handler.addExactPath("/ping", PingHandler::handlePingRequest);
        if (vc.configuration().server.serveStaticResources) {
            // Catch-all handler; must be last if present.
            handler.addPrefixPath("/", createStaticResourceHandler());
        }
        final SetHeaderHandler headerHandler =
                new SetHeaderHandler(handler, Headers.SERVER_STRING, "divolte");
        final HttpHandler canonicalPathHandler = new CanonicalPathHandler(headerHandler);
        final GracefulShutdownHandler rootHandler = new GracefulShutdownHandler(
                vc.configuration().server.useXForwardedFor ?
                new ProxyAdjacentPeerAddressHandler(canonicalPathHandler) : canonicalPathHandler
                );

        shutdownHandler = rootHandler;
        undertow = Undertow.builder()
                           .addHttpListener(port, host)
                           .setHandler(rootHandler)
                           .build();
    }

    private TrackingJavaScriptResource loadTrackingJavaScript(final ValidatedConfiguration vc) {
        try {
            return new TrackingJavaScriptResource(vc);
        } catch (final IOException e) {
            throw new RuntimeException("Could not precompile tracking JavaScript.", e);
        }
    }

    private HttpHandler createStaticResourceHandler() {
        final ResourceManager staticResources =
                new ClassPathResourceManager(getClass().getClassLoader(), "static");
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
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        logger.info("Starting server on {}:{}", host, port);
        undertow.start();
    }

    public void shutdown() {
        try {
            logger.info("Stopping HTTP server.");
            shutdownHandler.shutdown();
            shutdownHandler.awaitShutdown(HTTP_SHUTDOWN_GRACE_PERIOD_MILLIS);
            undertow.stop();
        } catch (Exception ie) {
            Thread.currentThread().interrupt();
        }

        logger.info("Stopping thread pools.");
        processingPool.stop();

        logger.info("Closing HDFS filesystem connection.");
        try {
            FileSystem.closeAll();
        } catch (IOException ioe) {
            logger.warn("Failed to cleanly close HDFS file system.", ioe);
        }
    }

    public static void main(final String[] args) {
        final ValidatedConfiguration vc = new ValidatedConfiguration(ConfigFactory::load);
        if (!vc.isValid()) {
            System.err.println("There are configuration errors. Details:");
            for (ConfigException ce : vc.errors()) {
                System.err.println(ce.getMessage());
            }
            System.exit(1);
        }

        // Tell Undertow to use Slf4J for logging by default.
        if (null == System.getProperty("org.jboss.logging.provider")) {
            System.setProperty("org.jboss.logging.provider", "slf4j");
        }
        new Server(vc).run();
    }
}
