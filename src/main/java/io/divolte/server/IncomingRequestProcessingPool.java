package io.divolte.server;

import io.divolte.server.CookieValues.CookieValue;
import io.divolte.server.hdfs.HdfsFlushingPool;
import io.divolte.server.kafka.KafkaFlushingPool;
import io.divolte.server.processing.ProcessingPool;
import io.undertow.server.HttpServerExchange;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@ParametersAreNonnullByDefault
final class IncomingRequestProcessingPool extends ProcessingPool<IncomingRequestProcessor, HttpServerExchange> {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessingPool.class);

    public IncomingRequestProcessingPool() {
        this(ConfigFactory.load());
    }

    public IncomingRequestProcessingPool(final Config config) {
        this (
                config.getInt("divolte.incoming_request_processor.threads"),
                config.getInt("divolte.incoming_request_processor.max_write_queue"),
                config.getDuration("divolte.incoming_request_processor.max_enqueue_delay", TimeUnit.MILLISECONDS),
                config,
                schemaFromConfig(config),
                config.getBoolean("divolte.kafka_flusher.enabled") ? new KafkaFlushingPool(config) : null,
                config.getBoolean("divolte.hdfs_flusher.enabled") ? new HdfsFlushingPool(config, schemaFromConfig(config)) : null
                );
    }

    public IncomingRequestProcessingPool(
            final int numThreads,
            final int maxQueueSize,
            final long maxEnqueueDelay,
            final Config config,
            final Schema schema,
            @Nullable final KafkaFlushingPool kafkaFlushingPool,
            @Nullable final HdfsFlushingPool hdfsFlushingPool) {
        super(
                numThreads,
                maxQueueSize,
                maxEnqueueDelay,
                "Incoming Request Processor",
                () -> new IncomingRequestProcessor(config, kafkaFlushingPool, hdfsFlushingPool, schema));
    }

    private static Schema schemaFromConfig(final Config config) {
        try {
            final Parser parser = new Schema.Parser();
            if (config.hasPath("divolte.tracking.schema_file")) {
                final String schemaFileName = config.getString("divolte.tracking.schema_file");
                logger.info("Using Avro schema from configuration: {}", schemaFileName);
                return parser.parse(new File(schemaFileName));
            } else {
                logger.info("Using built in default Avro schema.");
                return parser.parse(Resources.toString(Resources.getResource("DefaultEventRecord.avsc"), StandardCharsets.UTF_8));
            }
        } catch(IOException ioe) {
            logger.error("Failed to load Avro schema file.");
            throw new RuntimeException("Failed to load Avro schema file.", ioe);
        }
    }

    public void enqueueIncomingExchangeForProcessing(final CookieValue partyId, final HttpServerExchange exchange) {
        enqueue(partyId.value, exchange);
    }

    @ParametersAreNonnullByDefault
    static final class HttpServerExchangeWithPartyId {
        final CookieValue partyId;
        final HttpServerExchange exchange;

        public HttpServerExchangeWithPartyId(final CookieValue partyId,
                final HttpServerExchange exchange) {
            this.partyId = Objects.requireNonNull(partyId);
            this.exchange = Objects.requireNonNull(exchange);
        }
    }
}
