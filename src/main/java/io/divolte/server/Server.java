package io.divolte.server;

import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.CanonicalPathHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.ProxyPeerAddressHandler;
import io.undertow.server.handlers.SetHeaderHandler;
import io.undertow.server.handlers.cache.DirectBufferCache;
import io.undertow.server.handlers.resource.CachingResourceManager;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.server.handlers.resource.ResourceManager;
import io.undertow.util.Headers;

import java.time.Duration;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@ParametersAreNonnullByDefault
public class Server implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final Undertow undertow;

    private final String host;
    private final int port;

    public Server(final Config config) {
        host = config.getString("divolte.server.host");
        port = config.getInt("divolte.server.port");

        final DivolteEventHandler divolteEventHandler = new DivolteEventHandler(config);

        final PathHandler handler = new PathHandler();
        handler.addExactPath("/ping", PingHandler::handlePingRequest);
        handler.addExactPath("/event", divolteEventHandler::handleEventRequest);
        handler.addPrefixPath("/", createStaticResourceHandler());
        final SetHeaderHandler headerHandler =
                new SetHeaderHandler(handler, Headers.SERVER_STRING, "divolte");
        final HttpHandler canonicalPathHandler = new CanonicalPathHandler(headerHandler);
        final HttpHandler rootHandler = config.getBoolean("divolte.server.use_x_forwarded_for") ?
                new ProxyPeerAddressHandler(canonicalPathHandler) : canonicalPathHandler;

        undertow = Undertow.builder()
                           .addHttpListener(port, host)
                           .setHandler(rootHandler)
                           .setServerOption(UndertowOptions.RECORD_REQUEST_START_TIME, true)
                           .build();
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
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("Stopping server.");
                    undertow.stop();
                }
        ));
        logger.info("Starting server on {}:{}", host, port);
        undertow.start();
    }

    public static void main(final String[] args) {
        // Tell Undertow to use Slf4J for logging by default.
        if (null == System.getProperty("org.jboss.logging.provider")) {
            System.setProperty("org.jboss.logging.provider", "slf4j");
        }
        new Server(ConfigFactory.load()).run();
    }
}
