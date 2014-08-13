package io.divolte.server;

import static io.divolte.server.ConcurrentUtils.*;
import io.divolte.server.hdfs.HdfsFlushingPool;
import io.divolte.server.kafka.KafkaFlushingPool;
import io.undertow.server.HttpServerExchange;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@ParametersAreNonnullByDefault
final class IncomingRequestProcessor {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessor.class);

    private final BlockingQueue<HttpServerExchangeWithPartyId> queue;
    @Nullable
    private final KafkaFlushingPool kafkaFlushingPool;
    @Nullable
    private final HdfsFlushingPool hdfsFlushingPool;

    private final GenericRecordMaker maker;

    public IncomingRequestProcessor(final Config config,
                                    @Nullable final KafkaFlushingPool kafkaFlushingPool,
                                    @Nullable final HdfsFlushingPool hdfsFlushingPool,
                                    final Schema schema) {
        this.queue = new LinkedBlockingQueue<>();
        this.kafkaFlushingPool = kafkaFlushingPool;
        this.hdfsFlushingPool = hdfsFlushingPool;

        final Config schemaMappingConfig = schemaMappingConfigFromConfig(Objects.requireNonNull(config));
        this.maker = new GenericRecordMaker(Objects.requireNonNull(schema), schemaMappingConfig, config);
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

    public Runnable getQueueReader() {
        return microBatchingQueueDrainer(queue, this::processExchange);
    }

    private void processExchange(final HttpServerExchangeWithPartyId exchange) {
        final GenericRecord avroRecord = maker.makeRecordFromExchange(exchange.exchange);
        final AvroRecordBuffer avroBuffer = AvroRecordBuffer.fromRecord(exchange.partyId, avroRecord);

        if (null != kafkaFlushingPool) {
            kafkaFlushingPool.enqueueRecord(avroBuffer);
        }
        if (null != hdfsFlushingPool) {
            hdfsFlushingPool.enqueueRecordsForFlushing(avroBuffer);
        }
    }

    public void add(String partyId, HttpServerExchange exchange) {
        queue.add(new HttpServerExchangeWithPartyId(partyId, exchange));
    }

    @ParametersAreNonnullByDefault
    private static final class HttpServerExchangeWithPartyId {
        final String partyId;
        final HttpServerExchange exchange;

        public HttpServerExchangeWithPartyId(final String partyId,
                                             final HttpServerExchange exchange) {
            this.partyId = Objects.requireNonNull(partyId);
            this.exchange = Objects.requireNonNull(exchange);
        }
    }
}
