package io.divolte.server;

import static io.divolte.server.DivolteEventHandler.*;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;

/*
 * This class is called maker, because builder was already taken by Avro itself.
 */
@ParametersAreNonnullByDefault
@NotThreadSafe
final class GenericRecordMaker {
    private final static Logger logger = LoggerFactory.getLogger(GenericRecordMaker.class);

    private final String sessionIdCookie;

    private final Schema schema;
    private final Map<String, Pattern> regexes;
    private final List<FieldSetter> setters;

    private final LoadingCache<String,ReadableUserAgent> uaLookupCache;


    public GenericRecordMaker(Schema schema, Config config) {
        this (schema, config, config);
    }

    public GenericRecordMaker(Schema schema, Config schemaConfig, Config globalConfig) {
        Objects.requireNonNull(schemaConfig);
        Objects.requireNonNull(globalConfig);

        this.sessionIdCookie = globalConfig.getString("divolte.tracking.session_cookie");

        final int version = schemaConfig.getInt("divolte.tracking.schema_mapping.version");
        checkVersion(version);

        this.regexes = regexMapFromConfig(schemaConfig);
        this.setters = setterListFromConfig(schemaConfig);

        this.schema = Objects.requireNonNull(schema);

        final UserAgentStringParser parser = parserBasedOnTypeConfig(globalConfig.getString("divolte.tracking.ua_parser.type"));
        this.uaLookupCache = sizeBoundCacheFromLoadingFunction(parser::parse, globalConfig.getInt("divolte.tracking.ua_parser.cache_size"));

        logger.info("User agent parser data version: {}", parser.getDataVersion());
    }

    private UserAgentStringParser parserBasedOnTypeConfig(String type) {
        switch (type) {
        case "caching_and_updating":
            logger.info("Using caching and updating user agent parser.");
            return UADetectorServiceFactory.getCachingAndUpdatingParser();
        case "online_updating":
            logger.info("Using online updating user agent parser.");
            return UADetectorServiceFactory.getOnlineUpdatingParser();
        case "non_updating":
            logger.info("Using non-updating (resource module based) user agent parser.");
            return UADetectorServiceFactory.getResourceModuleParser();
        default:
            throw new RuntimeException("Invalid user agent parser type. Valid values are: caching_and_updating, online_updating, non_updating.");
        }
    }

    private FieldSetter fieldSetterFromConfig(final Entry<String, ConfigValue> entry) {
        final String name = entry.getKey();
        final ConfigValue value = entry.getValue();


        switch (value.valueType()) {
        case STRING:
            return simpleFieldSetterForConfig(name, value);
        case OBJECT:
            Config subConfig = ((ConfigObject) value).toConfig();
            if (!subConfig.hasPath("type")) {
                throw new SchemaMappingException("Missing type property on configuration for field %s.", name);
            }

            final String type = subConfig.getString("type");

            return complexFieldSetterForConfig(name, type, subConfig);
        default:
            throw new SchemaMappingException("Schema mapping for fields can only be of type STRING or OBJECT. Found %s.", value.valueType());
        }
    }

    private FieldSetter complexFieldSetterForConfig(final String name, final String type, final Config config) {
        switch (type) {
        case "cookie":
            return (b,e,c) -> Optional.ofNullable(e.getRequestCookies().get(config.getString("name"))).ifPresent((val) -> b.set(name, val.getValue()));
        case "regex_group":
            return regexGroupFieldSetter(name, config);
        case "regex_name":
            return regexNameFieldSetter(name, config);
        default:
            throw new SchemaMappingException("Unknown mapping type: %s for field %s.", type, name);
        }
    }

    private FieldSetter regexNameFieldSetter(final String name, final Config config) {
        final List<String> regexNames = config.getStringList("regexes");
        final String fieldName = config.getString("field");
        final StringValueExtractor fieldExtractor = fieldExtractorForName(fieldName);

        return (b, e, c) ->
            fieldExtractor.extract(e).ifPresent((val) ->
            regexNames.stream()
            .filter((nm) -> matcherFromContext(nm, fieldName, val, c).matches())
            .findFirst()
            .ifPresent((nm) -> b.set(name, nm)
            ));
    }

    private FieldSetter regexGroupFieldSetter(final String name, final Config config) {
        final String regexName = config.getString("regex");
        final String fieldName = config.getString("field");
        final String groupName = config.getString("group");
        final StringValueExtractor fieldExtractor = fieldExtractorForName(fieldName);

        return (b, e, c) ->
            fieldExtractor.extract(e)
            .ifPresent((val) ->
                groupFromMatcher(matcherFromContext(regexName, fieldName, val, c), groupName)
                .ifPresent((match) ->
                    b.set(name, match))
            );
    }

    private StringValueExtractor fieldExtractorForName(final String name) {
        switch (name) {
        case "userAgent":
            return (e) -> Optional.ofNullable(e.getRequestHeaders().getFirst(Headers.USER_AGENT));
        case "remoteHost":
            return (e) -> Optional.ofNullable(e.getSourceAddress())
                                  .flatMap((a) -> Optional.ofNullable(a.getHostString()));
        case "referer":
            return (e) -> Optional.ofNullable(e.getQueryParameters().get("r")).map(Deque::getFirst);
        case "location":
            return (e) -> Optional.ofNullable(e.getQueryParameters().get("l")).map(Deque::getFirst);
        default:
            throw new SchemaMappingException("Only userAgent, remoteHost, referer and location fields can be used for regex matchers. Found %s.", name);
        }
    }

    private FieldSetter simpleFieldSetterForConfig(final String name, final ConfigValue value) {
        final StringValueExtractor uaExtractor = fieldExtractorForName("userAgent");
        final StringValueExtractor remoteHostExtractor = fieldExtractorForName("remoteHost");
        final StringValueExtractor refererExtractor = fieldExtractorForName("referer");
        final StringValueExtractor locationExtractor = fieldExtractorForName("location");

        switch ((String) value.unwrapped()) {
        case "firstInSession":
            return (b, e, c) -> b.set(name, !e.getRequestCookies().containsKey(sessionIdCookie));
        case "timestamp":
            return (b, e, c) -> b.set(name, e.getAttachment(REQUEST_START_TIME_KEY));
        case "userAgent":
            return (b, e, c) -> uaExtractor.extract(e).ifPresent((ua) -> b.set(name, ua) );
        case "userAgentName":
            return (b, e, c) -> uaExtractor.extract(e).map(this::parseUnchecked).ifPresent((uan) -> b.set(name, uan.getName()));
        case "userAgentFamily":
            return (b, e, c) -> uaExtractor.extract(e).map(this::parseUnchecked).ifPresent((uan) -> b.set(name, uan.getFamily().getName()));
        case "userAgentVendor":
            return (b, e, c) -> uaExtractor.extract(e).map(this::parseUnchecked).ifPresent((uan) -> b.set(name, uan.getProducer()));
        case "userAgentType":
            return (b, e, c) -> uaExtractor.extract(e).map(this::parseUnchecked).ifPresent((uan) -> b.set(name, uan.getType().getName()));
        case "userAgentVersion":
            return (b, e, c) -> uaExtractor.extract(e).map(this::parseUnchecked).ifPresent((uan) -> b.set(name, uan.getVersionNumber().toVersionString()));
        case "userAgentDeviceCategory":
            return (b, e, c) -> uaExtractor.extract(e).map(this::parseUnchecked).ifPresent((uan) -> b.set(name, uan.getDeviceCategory().getName()));
        case "userAgentOsFamily":
            return (b, e, c) -> uaExtractor.extract(e).map(this::parseUnchecked).ifPresent((uan) -> b.set(name, uan.getOperatingSystem().getFamily().getName()));
        case "userAgentOsVersion":
            return (b, e, c) -> uaExtractor.extract(e).map(this::parseUnchecked).ifPresent((uan) -> b.set(name, uan.getOperatingSystem().getVersionNumber().toVersionString()));
        case "userAgentOsVendor":
            return (b, e, c) -> uaExtractor.extract(e).map(this::parseUnchecked).ifPresent((uan) -> b.set(name, uan.getOperatingSystem().getProducer()));
        case "remoteHost":
            return (b, e, c) -> remoteHostExtractor.extract(e).ifPresent((rh) -> b.set(name, rh));
        case "referer":
            return (b, e, c) -> refererExtractor.extract(e).ifPresent((ref) -> b.set(name, ref));
        case "location":
            return (b, e, c) -> locationExtractor.extract(e).ifPresent((loc) -> b.set(name, loc));
        case "viewportPixelWidth":
            return (b, e, c) -> Optional.ofNullable(e.getQueryParameters().get("w")).map(Deque::getFirst).map(this::parseIntOrNull).ifPresent((vw) -> b.set(name, vw));
        case "viewportPixelHeight":
            return (b, e, c) -> Optional.ofNullable(e.getQueryParameters().get("h")).map(Deque::getFirst).map(this::parseIntOrNull).ifPresent((vh) -> b.set(name, vh));
        case "screenPixelWidth":
            return (b, e, c) -> Optional.ofNullable(e.getQueryParameters().get("i")).map(Deque::getFirst).map(this::parseIntOrNull).ifPresent((sw) -> b.set(name, sw));
        case "screenPixelHeight":
            return (b, e, c) -> Optional.ofNullable(e.getQueryParameters().get("j")).map(Deque::getFirst).map(this::parseIntOrNull).ifPresent((sh) -> b.set(name, sh));
        case "partyId":
            return (b, e, c) -> b.set(name, e.getAttachment(PARTY_COOKIE_KEY).value);
        case "sessionId":
            return (b, e, c) -> b.set(name, e.getAttachment(SESSION_COOKIE_KEY).value);
        case "pageViewId":
            return (b, e, c) -> b.set(name, e.getAttachment(PAGE_VIEW_ID_KEY));
        default:
            throw new SchemaMappingException("Unknown field in schema mapping: %s", value);
        }
    }

    private List<FieldSetter> setterListFromConfig(final Config config) {
        if (!config.hasPath("divolte.tracking.schema_mapping.fields")) {
            throw new SchemaMappingException("Schema mapping configuration has no field mappings.");
        }

        final Set<Entry<String, ConfigValue>> entrySet = config.getConfig("divolte.tracking.schema_mapping.fields").root().entrySet();

        return entrySet.stream()
        .map(this::fieldSetterFromConfig)
        .collect(Collectors.toCollection(() -> new ArrayList<>(entrySet.size())));
    }

    private Map<String,Pattern> regexMapFromConfig(final Config config) {
        return config.hasPath("divolte.tracking.schema_mapping.regexes") ?
        config.getConfig("divolte.tracking.schema_mapping.regexes").root().entrySet().stream().collect(
                Collectors.<Entry<String,ConfigValue>, String, Pattern>toMap(
                Entry::getKey,
                (e) -> {
                    if (e.getValue().valueType() != ConfigValueType.STRING) {
                        throw new SchemaMappingException("Regexes config elements must be of type STRING. Found %s of type %s.", e.getKey(), e.getValue().valueType());
                    }
                    return Pattern.compile((String) e.getValue().unwrapped());
                })) : Collections.emptyMap();
    }

    private void checkVersion(final int version) {
        if (version != 1) {
            throw new SchemaMappingException("Unsupported schema mapping configuration version: %d", version);
        }
    }

    private Integer parseIntOrNull(final String s) {
        try {
            return Integer.valueOf(s);
        } catch(NumberFormatException nfe) {
            return null;
        }
    }

    private Matcher matcherFromContext(final String regex, final String field, final String value, final Map<String, Matcher> context) {
        return context.computeIfAbsent(
                regex + field,
                (ignored) -> regexes.get(regex).matcher(value));
    }

    private Optional<String> groupFromMatcher(final Matcher matcher, final String group) {
        return matcher.matches() ? Optional.ofNullable(matcher.group(group)) : Optional.empty();
    }

    @FunctionalInterface
    private interface FieldSetter {
        void setFields(GenericRecordBuilder builder, HttpServerExchange exchange, Map<String, Matcher> context);
    }

    @FunctionalInterface
    private interface StringValueExtractor {
        Optional<String> extract(HttpServerExchange exchange);
    }

    public GenericRecord makeRecordFromExchange(final HttpServerExchange exchange) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        Map<String, Matcher> context = new HashMap<>();
        setters.forEach((s) -> s.setFields(builder, exchange, context));
        return builder.build();
    }

    @ParametersAreNonnullByDefault
    public static class SchemaMappingException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public SchemaMappingException(String message) {
            super(message);
        }

        public SchemaMappingException(String message, Object... args) {
            this(String.format(message, args));
        }
    }

    private <K,V> LoadingCache<K, V> sizeBoundCacheFromLoadingFunction(Function<K, V> loader, int size) {
        return CacheBuilder
                .newBuilder()
                .maximumSize(size)
                .initialCapacity(size)
                .build(new CacheLoader<K, V>() {
                    @Override
                    public V load(K key) throws Exception {
                        return loader.apply(key);
                    }
                });
    }

    private ReadableUserAgent parseUnchecked(String key) {
        try {
            return uaLookupCache.get(key);
        } catch (ExecutionException e) {
            logger.warn("Failed to parse user agent string for: {}", key);
            return null;
        }
    }
}
