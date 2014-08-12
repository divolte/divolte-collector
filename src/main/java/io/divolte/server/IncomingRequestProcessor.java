package io.divolte.server;

import static io.divolte.server.ConcurrentUtils.*;
import io.divolte.server.hdfs.HdfsFlushingPool;
import io.divolte.server.kafka.KafkaFlushingPool;
import io.undertow.server.HttpServerExchange;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.typesafe.config.Config;

@ParametersAreNonnullByDefault
final class IncomingRequestProcessor {
    private final BlockingQueue<HttpServerExchangeWithPartyId> queue;
    private final KafkaFlushingPool kafkaFlushingPool;
    private final HdfsFlushingPool hdfsFlushingPool;

    private final GenericRecordMaker maker;

    public IncomingRequestProcessor(final Config schemaMappingConfig, final KafkaFlushingPool kafkaFlushingPool, final HdfsFlushingPool hdfsFlushingPool, final Schema schema) {
        this.queue = new LinkedBlockingQueue<>();
        this.kafkaFlushingPool = Objects.requireNonNull(kafkaFlushingPool);
        this.hdfsFlushingPool = Objects.requireNonNull(hdfsFlushingPool);

        this.maker = new GenericRecordMaker(schema, schemaMappingConfig);
    }

    public Runnable getQueueReader() {
        return microBatchingQueueDrainer(queue, this::processExchange);
    }

    private void processExchange(final HttpServerExchangeWithPartyId exchange) {
        final GenericRecord avroRecord = maker.makeRecordFromExchange(exchange.exchange);
        final AvroRecordBuffer avroBuffer = AvroRecordBuffer.fromRecord(exchange.partyId, avroRecord);
        kafkaFlushingPool.enqueueRecord(avroBuffer);
        hdfsFlushingPool.enqueueRecordsForFlushing(avroBuffer);
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
