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

import com.typesafe.config.Config;
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

    public Server(final Config config) {
        this(config, (e,b,r) -> {});
    }

    Server(final Config config, IncomingRequestListener listener) {
        host = config.getString("divolte.server.host");
        port = config.getInt("divolte.server.port");

        processingPool = new IncomingRequestProcessingPool(config, listener);
        final ServerSideCookieEventHandler serverSideCookieEventHandler =
                new ServerSideCookieEventHandler(config, processingPool);
        final ClientSideCookieEventHandler clientSideCookieEventHandler =
                new ClientSideCookieEventHandler(processingPool);
        final TrackingJavaScriptResource trackingJavaScript = loadTrackingJavaScript(config);
        final HttpHandler javascriptHandler = new AllowedMethodsHandler(new JavaScriptHandler(trackingJavaScript), Methods.GET);

        final PathHandler handler = new PathHandler();
        handler.addExactPath("/csc-event", clientSideCookieEventHandler::handleEventRequest);
        handler.addExactPath("/ssc-event", serverSideCookieEventHandler::handleEventRequest);
        handler.addExactPath('/' + trackingJavaScript.getScriptName(), javascriptHandler);
        handler.addExactPath("/ping", PingHandler::handlePingRequest);
        if (config.getBoolean("divolte.server.serve_static_resources")) {
            // Catch-all handler; must be last if present.
            handler.addPrefixPath("/", createStaticResourceHandler());
        }
        final SetHeaderHandler headerHandler =
                new SetHeaderHandler(handler, Headers.SERVER_STRING, "divolte");
        final HttpHandler canonicalPathHandler = new CanonicalPathHandler(headerHandler);
        final GracefulShutdownHandler rootHandler = new GracefulShutdownHandler(
                config.getBoolean("divolte.server.use_x_forwarded_for") ?
                new ProxyAdjacentPeerAddressHandler(canonicalPathHandler) : canonicalPathHandler
                );

        shutdownHandler = rootHandler;
        undertow = Undertow.builder()
                           .addHttpListener(port, host)
                           .setHandler(rootHandler)
                           .build();
    }

    private TrackingJavaScriptResource loadTrackingJavaScript(final Config config) {
        try {
            return new TrackingJavaScriptResource(config);
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
        // Tell Undertow to use Slf4J for logging by default.
        if (null == System.getProperty("org.jboss.logging.provider")) {
            System.setProperty("org.jboss.logging.provider", "slf4j");
        }
        new Server(ConfigFactory.load()).run();
    }
}
