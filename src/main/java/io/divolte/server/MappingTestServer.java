package io.divolte.server;

import static io.divolte.server.IncomingRequestProcessor.*;
import io.divolte.server.ip2geo.ExternalDatabaseLookupService;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.recordmapping.DslRecordMapper;
import io.divolte.server.recordmapping.RecordMapper;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.streams.ChannelInputStream;

import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@ParametersAreNonnullByDefault
public class MappingTestServer {
    private static final Logger log = LoggerFactory.getLogger(MappingTestServer.class);

    private final RecordMapper mapper;
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
        final Config config = ConfigFactory.load();

        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);
        mapper = new DslRecordMapper(vc, mappingFilename, schema, Optional.ofNullable(lookupServiceFromConfig(vc)));

        final HttpHandler handler = new AllowedMethodsHandler(this::handleEvent, Methods.POST);
        undertow = Undertow.builder()
                .addHttpListener(port, host)
                .setHandler(handler)
                .build();
    }

    private Schema loadSchema(final String schemaFilename) throws IOException {
        final Parser parser = new Schema.Parser();
        return parser.parse(new File(schemaFilename));
    }

    @Nullable
    private static LookupService lookupServiceFromConfig(final ValidatedConfiguration vc) {
        return vc.configuration().tracking.ip2geoDatabase
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

    private void handleEvent(HttpServerExchange exchange) throws Exception {
        final ChannelInputStream cis = new ChannelInputStream(exchange.getRequestChannel());
        final Map<String,Object> payload = JSON.std.<String>mapFrom(cis);
        final String generatedPageViewId = CookieValues.generate().value;

        final BrowserEventData eventData = new BrowserEventData(
                get(payload, "corrupt", Boolean.class).orElse(false),
                get(payload, "party_id", String.class).flatMap(CookieValues::tryParse).orElse(CookieValues.generate()),
                get(payload, "session_id", String.class).flatMap(CookieValues::tryParse).orElse(CookieValues.generate()),
                get(payload, "page_view_id", String.class).orElse(generatedPageViewId),
                get(payload, "event_id", String.class).orElse(generatedPageViewId + "0"),
                System.currentTimeMillis(),
                0L,
                get(payload, "new_party_id", Boolean.class).orElse(false),
                get(payload, "first_in_session", Boolean.class).orElse(false),
                get(payload, "location", String.class),
                get(payload, "referer", String.class),
                get(payload, "event_type", String.class),
                get(payload, "viewport_pixel_width", Integer.class),
                get(payload, "viewport_pixel_height", Integer.class),
                get(payload, "screen_pixel_width", Integer.class),
                get(payload, "screen_pixel_height", Integer.class),
                get(payload, "device_pixel_ratio", Integer.class),
                (name) -> get(payload, "param_" + name, String.class),
                () -> payload.entrySet()
                .stream()
                .filter((e) -> e.getKey().startsWith("param_"))
                .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                (e) -> (String) e.getValue())));

        exchange.putAttachment(EVENT_DATA_KEY, eventData);
        exchange.putAttachment(DUPLICATE_EVENT_KEY, get(payload, "duplicate", Boolean.class).orElse(false));

        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseChannel().write(ByteBuffer.wrap(mapper.newRecordFromExchange(exchange).toString().getBytes(StandardCharsets.UTF_8)));
        exchange.endExchange();
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<T> get(Map<String,Object> jsonResult, String key, Class<T> type) {
        final Object result = jsonResult.get(key);
        return result != null && result.getClass().isAssignableFrom(type) ? Optional.of((T) result) : Optional.empty();
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
