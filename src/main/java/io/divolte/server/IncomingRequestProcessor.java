package io.divolte.server;

import io.divolte.record.IncomingRequestRecord;
import io.divolte.server.kafka.KafkaFlushingPool;
import io.undertow.server.HttpServerExchange;

import javax.annotation.ParametersAreNonnullByDefault;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.avro.specific.SpecificRecord;

import static io.divolte.server.ConcurrentUtils.*;

@ParametersAreNonnullByDefault
final class IncomingRequestProcessor {
    private final BlockingQueue<HttpServerExchangeWithPartyId> queue;
    private final KafkaFlushingPool kafkaFlushingPool;
    private final HdfsFlushingPool hdfsFlushingPool;

    public IncomingRequestProcessor(final KafkaFlushingPool kafkaFlushingPool, final HdfsFlushingPool hdfsFlushingPool) {
        this.queue = new LinkedBlockingQueue<>();
        this.kafkaFlushingPool = Objects.requireNonNull(kafkaFlushingPool);
        this.hdfsFlushingPool = Objects.requireNonNull(hdfsFlushingPool);
    }

    public Runnable getQueueReader() {
        return microBatchingQueueDrainer(queue, this::processExchange);
    }

    private void processExchange(final HttpServerExchangeWithPartyId exchange) {
        final IncomingRequestRecord avroRecord = RecordUtil.recordFromExchange(exchange.exchange);
        final AvroRecordBuffer<SpecificRecord> avroBuffer = AvroRecordBuffer.fromRecord(exchange.partyId, avroRecord);
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
