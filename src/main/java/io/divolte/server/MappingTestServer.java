package io.divolte.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.ConfigFactory;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.ip2geo.ExternalDatabaseLookupService;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.recordmapping.DslRecordMapper;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.streams.ChannelInputStream;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Optional;

import static io.divolte.server.IncomingRequestProcessor.DUPLICATE_EVENT_KEY;

@ParametersAreNonnullByDefault
public class MappingTestServer {
    private static final Logger log = LoggerFactory.getLogger(MappingTestServer.class);

    private static final ObjectReader EVENT_PARAMETERS_READER = new ObjectMapper().reader();

    private final DslRecordMapper mapper;
    private final Undertow undertow;

    public static void main(final String[] args) throws IOException {
        final MappingTestServerOptionParser parser = new MappingTestServerOptionParser();
        try {
            final OptionSet options = parser.parse(args);
            if (options.has("help")) {
                parser.printHelpOn(System.out);
            } else {
                final String host = options.valueOf(parser.hostOption);
                final Integer port = options.valueOf(parser.portOption);
                final MappingTestServer server = new MappingTestServer(options.valueOf(parser.schemaOption),
                                                                       options.valueOf(parser.mappingOption),
                                                                       port, host);
                Runtime.getRuntime().addShutdownHook(new Thread(server::stopServer));
                log.info("Starting server on {}:{}...", host, port);
                server.runServer();
            }
        } catch (final OptionException e) {
            System.err.println(e.getLocalizedMessage());
            parser.printHelpOn(System.err);
        }
    }

    public MappingTestServer(final String schemaFilename, final String mappingFilename, final int port, final String host) throws IOException {
        final Schema schema = loadSchema(schemaFilename);
        final ValidatedConfiguration vc = new ValidatedConfiguration(ConfigFactory::load);
        mapper = new DslRecordMapper(vc, mappingFilename, schema, Optional.ofNullable(lookupServiceFromConfig(vc)));

        final HttpHandler handler = new AllowedMethodsHandler(this::handleEvent, Methods.POST);
        final HttpHandler rootHandler = new ProxyAdjacentPeerAddressHandler(handler);
        undertow = Undertow.builder()
                .addHttpListener(port, host)
                .setHandler(rootHandler)
                .build();
    }

    private Schema loadSchema(final String schemaFilename) throws IOException {
        final Parser parser = new Schema.Parser();
        return parser.parse(new File(schemaFilename));
    }

    @Nullable
    private static LookupService lookupServiceFromConfig(final ValidatedConfiguration vc) {
        return vc.configuration().global.mapper.ip2geoDatabase
                .map((path) -> {
                    try {
                        return new ExternalDatabaseLookupService(Paths.get(path));
                    } catch (final IOException e) {
                        throw new RuntimeException("Failed to configure GeoIP lookup service.", e);
                    }
                }).orElse(null);
    }

    private void runServer() {
        undertow.start();
    }

    private void stopServer() {
        undertow.stop();
    }

    private void handleEvent(final HttpServerExchange exchange) throws Exception {
        try (final ChannelInputStream cis = new ChannelInputStream(exchange.getRequestChannel())) {
            final JsonNode payload = EVENT_PARAMETERS_READER.readTree(cis);
            final String generatedPageViewId = DivolteIdentifier.generate().value;

            final DivolteEvent.BrowserEventData browserEventData = new DivolteEvent.BrowserEventData(
                    get(payload, "page_view_id", String.class).orElse(generatedPageViewId),
                    get(payload, "location", String.class),
                    get(payload, "referer", String.class),
                    get(payload, "viewport_pixel_width", Integer.class),
                    get(payload, "viewport_pixel_height", Integer.class),
                    get(payload, "screen_pixel_width", Integer.class),
                    get(payload, "screen_pixel_height", Integer.class),
                    get(payload, "device_pixel_ratio", Integer.class));
            final Instant now = Instant.now();
            final DivolteEvent divolteEvent = DivolteEvent.createBrowserEvent(
                    exchange,
                    get(payload, "corrupt", Boolean.class).orElse(false),
                    get(payload, "party_id", String.class).flatMap(DivolteIdentifier::tryParse).orElse(DivolteIdentifier.generate()),
                    get(payload, "session_id", String.class).flatMap(DivolteIdentifier::tryParse).orElse(DivolteIdentifier.generate()),
                    get(payload, "event_id", String.class).orElse(generatedPageViewId + "0"),
                    now, now,
                    get(payload, "new_party_id", Boolean.class).orElse(false),
                    get(payload, "first_in_session", Boolean.class).orElse(false),
                    get(payload, "event_type", String.class),
                    () -> get(payload, "parameters", JsonNode.class),
                    browserEventData);

            get(payload, "remote_host", String.class)
                .ifPresent(ip -> {
                    try {
                        final InetAddress address = InetAddress.getByName(ip);
                        // We have no way of knowing the port
                        exchange.setSourceAddress(new InetSocketAddress(address, 0));
                    } catch (final UnknownHostException e) {
                        log.warn("Could not parse remote host: " + ip, e);
                    }
                });

            exchange.putAttachment(DUPLICATE_EVENT_KEY, get(payload, "duplicate", Boolean.class).orElse(false));

            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
            exchange.getResponseChannel().write(ByteBuffer.wrap(mapper.newRecordFromExchange(divolteEvent).toString().getBytes(StandardCharsets.UTF_8)));
            exchange.endExchange();
        }
    }

    private static <T> Optional<T> get(final JsonNode jsonResult, final String key, final Class<T> type) {
        final Optional<JsonNode> fieldNode = Optional.ofNullable(jsonResult.get(key));
        return fieldNode.map(f -> {
            try {
                return EVENT_PARAMETERS_READER.treeToValue(f, type);
            } catch (final JsonProcessingException e) {
                log.info("Unable to map field: " + key, e);
                return null;
            }
        });
    }

    private static class MappingTestServerOptionParser extends OptionParser {
        final ArgumentAcceptingOptionSpec<String> schemaOption =
                acceptsAll(ImmutableList.of("schema", "s"), "The AVRO schema of the records to generate.")
                .withRequiredArg().required();
        final ArgumentAcceptingOptionSpec<String> mappingOption =
                acceptsAll(ImmutableList.of("mapping", "m"), "The mapping definition to use for processing requests.")
                .withRequiredArg().required();
        final ArgumentAcceptingOptionSpec<String> hostOption =
                acceptsAll(ImmutableList.of("host", "i"), "The address of the interface to listen on.")
                .withRequiredArg().defaultsTo("localhost");
        final ArgumentAcceptingOptionSpec<Integer> portOption =
                acceptsAll(ImmutableList.of("port", "p"), "The TCP port to listen on.")
                .withRequiredArg().ofType(Integer.class).defaultsTo(8390);

        public MappingTestServerOptionParser() {
            acceptsAll(ImmutableList.of("help", "h"), "Display this help.").forHelp();
        }
    }
}
