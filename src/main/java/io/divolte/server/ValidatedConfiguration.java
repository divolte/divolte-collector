/*
 * Copyright 2015 GoDataDriven B.V.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;

public final class ValidatedConfiguration {
    private final List<ConfigException> exceptions;
    private final DivolteConfiguration divolteConfiguration;

    public ValidatedConfiguration(Supplier<Config> configLoader) {
        final List<ConfigException> exceptions = new ArrayList<>();

        DivolteConfiguration divolteConfiguration;
        try {
            final Config config = configLoader.get();
            divolteConfiguration = validateAndLoad(config, exceptions);
        } catch(ConfigException ce) {
            exceptions.add(ce);
            divolteConfiguration = null;
        }

        this.exceptions = ImmutableList.copyOf(exceptions);
        this.divolteConfiguration = divolteConfiguration;
    }

    private static DivolteConfiguration validateAndLoad(final Config input, final List<ConfigException> exceptions) {
        final Config config = input.resolve();
        final ServerConfiguration server = new ServerConfiguration(
                getOrAddException(              config::getString,      "divolte.server.host",                          exceptions),
                getOrAddException(              config::getInt,         "divolte.server.port",                          exceptions),
                getOrAddException(              config::getBoolean,     "divolte.server.use_x_forwarded_for",           exceptions),
                getOrAddException(              config::getBoolean,     "divolte.server.serve_static_resources",        exceptions));

        final UaParserConfiguration uaParser = new UaParserConfiguration(
                getOrAddException(              config::getString,      "divolte.tracking.ua_parser.type",              exceptions),
                getOrAddException(              config::getInt,         "divolte.tracking.ua_parser.cache_size",        exceptions));

        final Optional<SchemaMappingConfiguration> schemaMapping = !config.hasPath("divolte.tracking.schema_mapping") ?
            Optional.empty() :
            Optional.of(new SchemaMappingConfiguration(
                getOrAddException(              config::getInt,         "divolte.tracking.schema_mapping.cache_size",   exceptions),
                getOrAddException(              config::getString,      "divolte.tracking.schema_mapping.type",         exceptions)));

        final TrackingConfiguration tracking = new TrackingConfiguration(
                getOrAddException(              config::getString,      "divolte.tracking.party_cookie",                exceptions),
                getOrAddException(              duration(config),       "divolte.tracking.party_timeout",               exceptions),
                getOrAddException(              config::getString,      "divolte.tracking.session_cookie",              exceptions),
                getOrAddException(              duration(config),       "divolte.tracking.session_timeout",             exceptions),
                getOptionalOrAddException(      config::getString,      "divolte.tracking.cookie_domain",               exceptions, config),
                uaParser,
                getOptionalOrAddException(      config::getString,      "divolte.tracking.ip2geo_database",             exceptions, config),
                getOptionalOrAddException(      config::getString,      "divolte.tracking.schema_file",                 exceptions, config),
                schemaMapping
                );

        final JavascriptConfiguration javascript = new JavascriptConfiguration(
                getOrAddException(              config::getString,      "divolte.javascript.name",                      exceptions),
                getOrAddException(              config::getBoolean,     "divolte.javascript.logging",                   exceptions),
                getOrAddException(              config::getBoolean,     "divolte.javascript.debug",                     exceptions)
                );

        if (!javascript.name.matches("^[A-Za-z0-9_-]+\\.js$")) {
            exceptions.add(
                    new ConfigException.Generic(
                            String.format("Script name (divolte.javascript.name) must contain only letters, "
                                    + "numbers, underscores and dashes. It must also end in '.js'. Found: %s",
                                    javascript.name)
                            ));
        }

        final IncomingRequestProcessorConfiguration incomingRequestProcessor = new IncomingRequestProcessorConfiguration(
                getOrAddException(              config::getInt,         "divolte.incoming_request_processor.threads",                 exceptions),
                getOrAddException(              config::getInt,         "divolte.incoming_request_processor.max_write_queue",         exceptions),
                getOrAddException(              duration(config),       "divolte.incoming_request_processor.max_enqueue_delay",       exceptions),
                getOrAddException(              config::getBoolean,     "divolte.incoming_request_processor.discard_corrupted",       exceptions),
                getOrAddException(              config::getInt,         "divolte.incoming_request_processor.duplicate_memory_size",   exceptions),
                getOrAddException(              config::getBoolean,     "divolte.incoming_request_processor.discard_duplicates",      exceptions)
                );

        final KafkaFlusherConfiguration kafkaFlusher = new KafkaFlusherConfiguration(
                getOrAddException(              config::getBoolean,     "divolte.kafka_flusher.enabled",                              exceptions),
                getOrAddException(              config::getInt,         "divolte.kafka_flusher.threads",                              exceptions),
                getOrAddException(              config::getInt,         "divolte.kafka_flusher.max_write_queue",                      exceptions),
                getOrAddException(              duration(config),       "divolte.kafka_flusher.max_enqueue_delay",                    exceptions),
                getOrAddException(              config::getString,      "divolte.kafka_flusher.topic",                                exceptions),
                getOrAddException((p) -> configToProperties(config, p), "divolte.kafka_flusher.producer",                             exceptions)
                );

        final HdfsConfiguration hdfs = new HdfsConfiguration(
                getOptionalOrAddException(      config::getString,      "divolte.hdfs_flusher.hdfs.uri",                              exceptions, config),
                getOrAddException(              config::getInt,         "divolte.hdfs_flusher.hdfs.replication",                      exceptions)
                );

        final FileStrategyConfiguration fileStrategy;
        final boolean simpleStrategyPresent = config.hasPath("divolte.hdfs_flusher.simple_rolling_file_strategy");
        final boolean sbStrategyPresent = config.hasPath("divolte.hdfs_flusher.session_binning_file_strategy");
        if (!simpleStrategyPresent && !sbStrategyPresent) {
            fileStrategy = null;
            exceptions.add(new ConfigException.Generic("Either simple_rolling_file_strategy or session_binning_file_strategy should be configured. None found."));
        } else if (sbStrategyPresent) {
            // if both strategies are present in the config, the session binning one takes precedence
            fileStrategy = new SessionBinningFileStrategyConfiguration(
                    getOrAddException(              config::getInt,      "divolte.hdfs_flusher.simple_rolling_file_strategy.sync_file_after_records",   exceptions),
                    getOrAddException(              duration(config),    "divolte.hdfs_flusher.simple_rolling_file_strategy.sync_file_after_duration",  exceptions),
                    getOrAddException(              config::getString,   "divolte.hdfs_flusher.simple_rolling_file_strategy.working_dir",               exceptions),
                    getOrAddException(              config::getString,   "divolte.hdfs_flusher.simple_rolling_file_strategy.publish_dir",               exceptions));
        } else {
            fileStrategy = new SimpleRollingFileStrategyConfiguration(
                    getOrAddException(              duration(config),    "divolte.hdfs_flusher.simple_rolling_file_strategy.roll_every",                exceptions),
                    getOrAddException(              config::getInt,      "divolte.hdfs_flusher.simple_rolling_file_strategy.sync_file_after_records",   exceptions),
                    getOrAddException(              duration(config),    "divolte.hdfs_flusher.simple_rolling_file_strategy.sync_file_after_duration",  exceptions),
                    getOrAddException(              config::getString,   "divolte.hdfs_flusher.simple_rolling_file_strategy.working_dir",               exceptions),
                    getOrAddException(              config::getString,   "divolte.hdfs_flusher.simple_rolling_file_strategy.publish_dir",               exceptions));
        }

        final HdfsFlusherConfiguration hdfsFlusher = new HdfsFlusherConfiguration(
                getOrAddException(              config::getBoolean,     "divolte.hdfs_flusher.enabled",                              exceptions),
                getOrAddException(              config::getInt,         "divolte.hdfs_flusher.threads",                              exceptions),
                getOrAddException(              config::getInt,         "divolte.hdfs_flusher.max_write_queue",                      exceptions),
                getOrAddException(              duration(config),       "divolte.hdfs_flusher.max_enqueue_delay",                    exceptions),
                hdfs,
                fileStrategy
                );

        return new DivolteConfiguration(server, tracking, javascript, incomingRequestProcessor, kafkaFlusher, hdfsFlusher);
    }

    private static Function<String,Duration> duration(final Config config) {
        return (p) -> Duration.ofMillis(config.getDuration(p, TimeUnit.MILLISECONDS));
    }

    private static <T> T getOrAddException(final Function<String,T> getter, final String path, final List<ConfigException> exceptions) {
        try {
            return getter.apply(path);
        } catch(ConfigException ce) {
            exceptions.add(ce);
            return null;
        }
    }

    private static <T> Optional<T> getOptionalOrAddException(final Function<String,T> getter, final String path, final List<ConfigException> exceptions, final Config config) {
        try {
            if (!config.hasPath(path)) {
                return Optional.empty();
            }
            return Optional.of(getter.apply(path));
        } catch(ConfigException ce) {
            exceptions.add(ce);
            return Optional.empty();
        }
    }

    private static final Joiner COMMA_JOINER = Joiner.on(',');

    private static Properties configToProperties(final Config config, final String path) {
        final Properties properties = new Properties();
        for (final Map.Entry<String,ConfigValue> entry : config.getConfig(path).entrySet()) {
            final ConfigValue configValue = entry.getValue();
            final String value;
            switch (configValue.valueType()) {
                case STRING:
                case BOOLEAN:
                case NUMBER:
                    value = configValue.unwrapped().toString();
                    break;
                case LIST:
                    final ConfigList configList = (ConfigList)configValue;
                    // We only need to support 'simple' types here.
                    value = COMMA_JOINER.join(configList.unwrapped());
                    break;
                case OBJECT:
                case NULL:
                default:
                    throw new IllegalStateException("Property type not supported for Kafka configuration: " + entry);
            }
            properties.setProperty(entry.getKey(), value);
        }
        return properties;
    }

    public DivolteConfiguration configuration() {
        if (exceptions.size() > 0) {
            throw new IllegalStateException("Attempt to access invalid configuration.");
        }
        return divolteConfiguration;
    }

    public List<ConfigException> errors() {
        return exceptions;
    }

    public boolean isValid() {
        return exceptions.size() == 0;
    }

    public final static class DivolteConfiguration {
        public final ServerConfiguration server;
        public final TrackingConfiguration tracking;
        public final JavascriptConfiguration javascript;
        public final IncomingRequestProcessorConfiguration incomingRequestProcessor;
        public final KafkaFlusherConfiguration kafkaFlusher;
        public final HdfsFlusherConfiguration hdfsFlusher;

        public DivolteConfiguration(
                final ServerConfiguration server,
                final TrackingConfiguration tracking,
                final JavascriptConfiguration javascript,
                final IncomingRequestProcessorConfiguration incomingRequestProcessor,
                final KafkaFlusherConfiguration kafkaFlusher,
                final HdfsFlusherConfiguration hdfsFlusher) {
            this.server = server;
            this.tracking = tracking;
            this.javascript = javascript;
            this.incomingRequestProcessor = incomingRequestProcessor;
            this.kafkaFlusher = kafkaFlusher;
            this.hdfsFlusher = hdfsFlusher;
        }
    }

    public final static class ServerConfiguration {
        public final String host;
        public final int port;
        public final boolean useXForwardedFor;
        public final boolean serveStaticResources;

        private ServerConfiguration(final String host, final int port, final boolean useXForwardedFor, final boolean serveStaticResources) {
            this.host = host;
            this.port = port;
            this.useXForwardedFor = useXForwardedFor;
            this.serveStaticResources = serveStaticResources;
        }
    }

    public final static class TrackingConfiguration {
        public final String partyCookie;
        public final Duration partyTimeout;
        public final String sessionCookie;
        public final Duration sessionTimeout;
        public final Optional<String> cookieDomain;
        public final UaParserConfiguration uaParser;
        public final Optional<String> ip2geoDatabase;
        public final Optional<String> schemaFile;
        public final Optional<SchemaMappingConfiguration> schemaMapping;

        private TrackingConfiguration(
                final String partyCookie,
                final Duration partyTimeout,
                final String sessionCookie,
                final Duration sessionTimeout,
                final Optional<String> cookieDomain,
                final UaParserConfiguration uaParser,
                final Optional<String> ip2geoDatabase,
                final Optional<String> schemaFile,
                final Optional<SchemaMappingConfiguration> schemaMapping) {
            this.partyCookie = partyCookie;
            this.partyTimeout = partyTimeout;
            this.sessionCookie = sessionCookie;
            this.sessionTimeout = sessionTimeout;
            this.cookieDomain = cookieDomain;
            this.uaParser = uaParser;
            this.ip2geoDatabase = ip2geoDatabase;
            this.schemaFile = schemaFile;
            this.schemaMapping = schemaMapping;
        }
    }

    public final static class UaParserConfiguration {
        public final String type;
        public final int cacheSize;

        private UaParserConfiguration(final String type, final int cacheSize) {
            this.type = type;
            this.cacheSize = cacheSize;
        }
    }

    public final static class SchemaMappingConfiguration {
        public final int version;
        public final String mappingScriptFile;

        private SchemaMappingConfiguration(final int version, final String mappingScriptFile) {
            this.version = version;
            this.mappingScriptFile = mappingScriptFile;
        }
    }

    public final static class JavascriptConfiguration {
        public final String name;
        public final boolean logging;
        public final boolean debug;

        private JavascriptConfiguration(final String name, final boolean logging, final boolean debug) {
            this.name = name;
            this.logging = logging;
            this.debug = debug;
        }
    }

    public final static class IncomingRequestProcessorConfiguration {
        public final int threads;
        public final int maxWriteQueue;
        public final Duration maxEnqueueDelay;
        public final boolean discardCorrupted;
        public final int duplicateMemorySize;
        public final boolean discardDuplicates;

        private IncomingRequestProcessorConfiguration(
                final int threads,
                final int maxWriteQueue,
                final Duration maxEnqueueDelay,
                final boolean discardCorrupted,
                final int duplicateMemorySize,
                final boolean discardDuplicates) {
            this.threads = threads;
            this.maxWriteQueue = maxWriteQueue;
            this.maxEnqueueDelay = maxEnqueueDelay;
            this.discardCorrupted = discardCorrupted;
            this.duplicateMemorySize = duplicateMemorySize;
            this.discardDuplicates = discardDuplicates;
        }
    }

    public static final class KafkaFlusherConfiguration {
        public final boolean enabled;
        public final int threads;
        public final int maxWriteQueue;
        public final Duration maxEnqueueDelay;
        public final String topic;
        public final Properties producer;

        private KafkaFlusherConfiguration(
                final boolean enabled, int threads,
                final int maxWriteQueue,
                final Duration maxEnqueueDelay,
                final String topic,
                final Properties producer) {
            this.enabled = enabled;
            this.threads = threads;
            this.maxWriteQueue = maxWriteQueue;
            this.maxEnqueueDelay = maxEnqueueDelay;
            this.topic = topic;
            this.producer = producer;
        }
    }

    public static final class HdfsFlusherConfiguration {
        public final boolean enabled;
        public final int threads;
        public final int maxWriteQueue;
        public final Duration maxEnqueueDelay;
        public final HdfsConfiguration hdfs;
        public final FileStrategyConfiguration fileStrategy;

        private HdfsFlusherConfiguration(
                final boolean enabled,
                final int threads,
                final int maxWriteQueue,
                final Duration maxEnqueueDelay,
                final HdfsConfiguration hdfs,
                final FileStrategyConfiguration fileStrategy) {
            this.enabled = enabled;
            this.threads = threads;
            this.maxWriteQueue = maxWriteQueue;
            this.maxEnqueueDelay = maxEnqueueDelay;
            this.hdfs = hdfs;
            this.fileStrategy = fileStrategy;
        }
    }

    public static final class HdfsConfiguration {
        public final Optional<String> uri;
        public final int replication;

        private HdfsConfiguration(Optional<String> uri, int replication) {
            this.uri = uri;
            this.replication = replication;
        }
    }

    public static class FileStrategyConfiguration {
        public final Types type;
        public final int syncDileAfterRecords;
        public final Duration syncFileAfterDuration;
        public final String workingDir;
        public final String publishDir;

        protected FileStrategyConfiguration (
                final Types type,
                final int syncFileAfterRecords,
                final Duration syncFileAfterDuration,
                final String workingDir,
                final String publishDir) {
            this.type = type;
            this.syncDileAfterRecords = syncFileAfterRecords;
            this.syncFileAfterDuration = syncFileAfterDuration;
            this.workingDir = workingDir;
            this.publishDir = publishDir;
        }

        public static enum Types {
            SIMPLE_ROLLING_FILE, SESSION_BINNING;
        }

        public SimpleRollingFileStrategyConfiguration asSimpleRollingFileStrategy() {
            if (Types.SIMPLE_ROLLING_FILE != type) throw new IllegalStateException("Attempt to cast FileStrategyConfiguration to wrong type.");
            return (SimpleRollingFileStrategyConfiguration) this;
        }

        public SessionBinningFileStrategyConfiguration asSessionBinningFileStrategy() {
            if (Types.SESSION_BINNING != type) throw new IllegalStateException("Attempt to cast FileStrategyConfiguration to wrong type.");
            return (SessionBinningFileStrategyConfiguration) this;
        }
    }

    public static final class SimpleRollingFileStrategyConfiguration extends FileStrategyConfiguration {
        public final Duration rollEvery;

        private SimpleRollingFileStrategyConfiguration(
                final Duration rollEvery,
                final int syncFileAfterRecords,
                final Duration syncFileAfterDuration,
                final String workingDir,
                final String publishDir) {
            super(Types.SIMPLE_ROLLING_FILE, syncFileAfterRecords, syncFileAfterDuration, workingDir, publishDir);
            this.rollEvery = rollEvery;
        }
    }

    public static final class SessionBinningFileStrategyConfiguration extends FileStrategyConfiguration {
        private SessionBinningFileStrategyConfiguration(
                final int syncFileAfterRecords,
                final Duration syncFileAfterDuration,
                final String workingDir,
                final String publishDir) {
            super(Types.SESSION_BINNING, syncFileAfterRecords, syncFileAfterDuration, workingDir, publishDir);
        }
    }
}
