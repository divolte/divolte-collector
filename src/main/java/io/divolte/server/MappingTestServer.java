package io.divolte.server;

import static io.divolte.server.BaseEventHandler.*;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.xnio.streams.ChannelInputStream;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.jr.ob.JSON;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class MappingTestServer {
    private final RecordMapper mapper;
    private final Undertow undertow;

    public static void main(String[] args) throws IOException {
        CliArgs cli = new CliArgs();
        new JCommander(cli, args);
        if (cli.help) {
            printUsage();
            System.exit(0);
        }

        if (cli.schemaAndMapping == null || cli.schemaAndMapping.size() != 2) {
            printUsage();
            System.exit(1);
        }

        MappingTestServer server = new MappingTestServer(cli.schemaAndMapping.get(0), cli.schemaAndMapping.get(1), cli.port, cli.host);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stopServer()));
        server.runServer();
    }

    public MappingTestServer(final String schemaFilename, final String mappingFilename, final int port, final String host) throws IOException {
        final Schema schema = loadSchema(schemaFilename);
        final Config config = ConfigFactory.load();

        mapper = new DslRecordMapper(config, mappingFilename, schema, Optional.ofNullable(lookupServiceFromConfig(config)));

        final HttpHandler handler = new AllowedMethodsHandler(this::handleEvent, Methods.POST);
        undertow = Undertow.builder()
                .addHttpListener(port, host)
                .setHandler(handler)
                .build();
    }

    private Schema loadSchema(final String schemaFilename) throws IOException {
        final Parser parser = new Schema.Parser();
        final String schemaFileName = schemaFilename;
        final Schema schema = parser.parse(new File(schemaFileName));
        return schema;
    }

    @Nullable
    private static LookupService lookupServiceFromConfig(final Config config) {
        return OptionalConfig.of(config::getString, "divolte.geodb")
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

        exchange.putAttachment(REQUEST_START_TIME_KEY, System.currentTimeMillis());

        exchange.putAttachment(DUPLICATE_EVENT_KEY, get(payload, "duplicate", Boolean.class).orElse(false));
        exchange.putAttachment(CORRUPT_EVENT_KEY, get(payload, "corrupt", Boolean.class).orElse(false));
        exchange.putAttachment(LOCATION_KEY, get(payload, "location", String.class));
        exchange.putAttachment(REFERER_KEY, get(payload, "referer", String.class));
        exchange.putAttachment(EVENT_TYPE_KEY, get(payload, "event_type", String.class));
        exchange.putAttachment(PARTY_COOKIE_KEY, get(payload, "party_id", String.class).flatMap((v) -> CookieValues.tryParse(v)).orElse(CookieValues.generate()));
        exchange.putAttachment(SESSION_COOKIE_KEY, get(payload, "session_id", String.class).flatMap((v) -> CookieValues.tryParse(v)).orElse(CookieValues.generate()));
        exchange.putAttachment(PAGE_VIEW_ID_KEY, get(payload, "page_view_id", String.class).orElse(generatedPageViewId));
        exchange.putAttachment(EVENT_ID_KEY, get(payload, "event_id", String.class).orElse(generatedPageViewId + "0"));
        exchange.putAttachment(FIRST_IN_SESSION_KEY, get(payload, "first_in_session", Boolean.class).orElse(false));
        exchange.putAttachment(VIEWPORT_PIXEL_WIDTH_KEY, get(payload, "viewport_pixel_width", Integer.class));
        exchange.putAttachment(VIEWPORT_PIXEL_HEIGHT_KEY, get(payload, "viewport_pixel_height", Integer.class));
        exchange.putAttachment(SCREEN_PIXEL_WIDTH_KEY, get(payload, "screen_pixel_width", Integer.class));
        exchange.putAttachment(SCREEN_PIXEL_HEIGHT_KEY, get(payload, "screen_pixel_height", Integer.class));
        exchange.putAttachment(DEVICE_PIXEL_RATIO_KEY, get(payload, "device_pixel_ratio", Integer.class));
        exchange.putAttachment(EVENT_PARAM_PRODUCER_KEY, (name) -> get(payload, "param_" + name, String.class));

        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseChannel().write(ByteBuffer.wrap(mapper.newRecordFromExchange(exchange).toString().getBytes(StandardCharsets.UTF_8)));
        exchange.endExchange();
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<T> get(Map<String,Object> jsonResult, String key, Class<T> type) {
        final Object result = jsonResult.get(key);
        return result != null && result.getClass().isAssignableFrom(type) ? Optional.of((T) result) : Optional.empty();
    }

    public static void printUsage() {
        System.err.println(
                  "Usage:\n"
                + "MappingTestServer <schema file> <mapping script file> [-p port]");
    }

    private static final class CliArgs {
        @Parameter(arity = 2, required = true)
        private List<String> schemaAndMapping;

        @Parameter(names = { "-p", "--port" }, description = "Port number", arity = 1, required = false)
        private Integer port = 8390;

        @Parameter(names = { "-i", "--host" }, description = "Host to bind on", arity = 1, required = false)
        private String host = "localhost";

        @Parameter(names = { "-h", "--help" }, description = "Help", help = true)
        private boolean help;
    }
}
