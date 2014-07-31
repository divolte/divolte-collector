package io.divolte.server;

import static io.divolte.server.ConcurrentUtils.*;
import io.divolte.record.IncomingRequestRecord;
import io.divolte.server.LocalFileFlushingPool.FilePosition;
import io.undertow.server.HttpServerExchange;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class IncomingRequestProcessor {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessor.class);

    private final LinkedBlockingQueue<HttpServerExchangeWithPartyId> queue;
    private final LocalFileFlushingPool localFlushingPool;

    public IncomingRequestProcessor(final LocalFileFlushingPool localFlushingPool) {
        this.queue = new LinkedBlockingQueue<>();
        this.localFlushingPool = localFlushingPool;
    }

    public Runnable getQueueReader() {
        return microBatchingQueueDrainer(queue, this::processExchange);
    }

    private void processExchange(final HttpServerExchangeWithPartyId exchange) {
        IncomingRequestRecord avroRecord = RecordUtil.recordFromExchange(exchange.exchange);
        AvroRecordBuffer<SpecificRecord> avroBuffer = AvroRecordBuffer.fromRecord(avroRecord);

        FilePosition filePosition = localFlushingPool.enqueueRecordForFlushing(exchange.partyId, avroBuffer);
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
