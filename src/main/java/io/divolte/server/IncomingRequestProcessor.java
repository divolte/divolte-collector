package io.divolte.server;

import static io.divolte.server.DivolteEventHandler.*;
import io.divolte.server.hdfs.HdfsFlushingPool;
import io.divolte.server.kafka.KafkaFlushingPool;
import io.divolte.server.processing.ItemProcessor;
import io.undertow.server.HttpServerExchange;

import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@ParametersAreNonnullByDefault
final class IncomingRequestProcessor implements ItemProcessor<HttpServerExchange> {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessor.class);

    @Nullable
    private final KafkaFlushingPool kafkaFlushingPool;
    @Nullable
    private final HdfsFlushingPool hdfsFlushingPool;

    private final GenericRecordMaker maker;

    public IncomingRequestProcessor(final Config config,
                                    @Nullable final KafkaFlushingPool kafkaFlushingPool,
                                    @Nullable final HdfsFlushingPool hdfsFlushingPool,
                                    final Schema schema) {

        this.kafkaFlushingPool = kafkaFlushingPool;
        this.hdfsFlushingPool = hdfsFlushingPool;

        final Config schemaMappingConfig = schemaMappingConfigFromConfig(Objects.requireNonNull(config));
        maker = new GenericRecordMaker(Objects.requireNonNull(schema), schemaMappingConfig, config);
    }

    private Config schemaMappingConfigFromConfig(final Config config) {
        final Config schemaMappingConfig;
        if (config.hasPath("divolte.tracking.schema_mapping")) {
            logger.info("Using schema mapping from configuration.");
            schemaMappingConfig = config;
        } else {
            logger.info("Using built in default schema mapping.");
            schemaMappingConfig = ConfigFactory.load("default-schema-mapping");
        }
        return schemaMappingConfig;
    }

    @Override
    public void process(final HttpServerExchange exchange) {
        final GenericRecord avroRecord = maker.makeRecordFromExchange(exchange);
        final AvroRecordBuffer avroBuffer = AvroRecordBuffer.fromRecord(
                exchange.getAttachment(PARTY_COOKIE_KEY),
                exchange.getAttachment(SESSION_COOKIE_KEY),
                exchange.getAttachment(REQUEST_START_TIME_KEY),
                avroRecord);

        if (null != kafkaFlushingPool) {
            kafkaFlushingPool.enqueueRecord(avroBuffer);
        }
        if (null != hdfsFlushingPool) {
            hdfsFlushingPool.enqueueRecordsForFlushing(avroBuffer);
        }

        // #toString() is quite expensive on Avro records
        if (logger.isDebugEnabled()) {
            logger.debug("Incoming request record:\n{}", avroRecord);
        }
    }
}
