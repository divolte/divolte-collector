package io.divolte.server;

import static io.divolte.server.ConcurrentUtils.*;
import io.divolte.record.IncomingRequestRecord;
import io.undertow.server.HttpServerExchange;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.avro.specific.SpecificRecord;

final class IncomingRequestProcessor {
    private final LinkedBlockingQueue<HttpServerExchangeWithPartyId> queue;
    private final HdfsFlushingPool hdfsFlushingPool;

    public IncomingRequestProcessor(final HdfsFlushingPool hdfsFlushingPool) {
        this.queue = new LinkedBlockingQueue<>();
        this.hdfsFlushingPool = hdfsFlushingPool;
    }

    public Runnable getQueueReader() {
        return microBatchingQueueDrainer(queue, this::processExchange);
    }

    private void processExchange(final HttpServerExchangeWithPartyId exchange) {
        IncomingRequestRecord avroRecord = RecordUtil.recordFromExchange(exchange.exchange);
        AvroRecordBuffer<SpecificRecord> avroBuffer = AvroRecordBuffer.fromRecord(avroRecord);

        hdfsFlushingPool.enqueueRecordForFlushing(exchange.partyId, avroBuffer);
    }

    public void add(String partyId, HttpServerExchange exchange) {
        queue.add(new HttpServerExchangeWithPartyId(partyId, exchange));
    }

    private final class HttpServerExchangeWithPartyId {
        final String partyId;
        final HttpServerExchange exchange;

        public HttpServerExchangeWithPartyId(String partyId, HttpServerExchange exchange) {
            this.partyId = partyId;
            this.exchange = exchange;
        }
    }
}
