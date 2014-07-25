package io.divolte.server;

import io.divolte.record.IncomingRequestRecord;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class IncomingRequestProcessor {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessor.class);
    
    private final LinkedBlockingQueue<HttpServerExchange> queue;

    public IncomingRequestProcessor() {
        this.queue = new LinkedBlockingQueue<>();
    }

    public void readQueue() {
        final int maxBatchSize = 100;
        final List<HttpServerExchange> batch = new ArrayList<>(maxBatchSize);
        
        while(true) {
            final int batchSize = queue.drainTo(batch, maxBatchSize);
            
            batch.forEach((exchange) -> {
                processExchange(exchange);
            });
            batch.clear();

            // if the batch was empty, block on the queue for some time until something is available
            final HttpServerExchange polled;
            if (batchSize == 0 && (polled = pollQuietly(queue, 1, TimeUnit.SECONDS)) != null) {
                batch.add(polled);
            }
        }
    }
    
    private void processExchange(final HttpServerExchange exchange) {
        IncomingRequestRecord avroRecord = RecordUtil.recordFromExchange(exchange);
        AvroRecordBuffer<IncomingRequestRecord> avroBuffer = AvroRecordBuffer.fromRecord(avroRecord);
        logger.debug("Serialized Avro record: {}", avroBuffer);
    }
    
    private static <E> E pollQuietly(final LinkedBlockingQueue<E> queue, long timeout, TimeUnit unit) {
        try {
            return queue.poll(timeout, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void add(HttpServerExchange exchange) {
        queue.add(exchange);
    }
}
