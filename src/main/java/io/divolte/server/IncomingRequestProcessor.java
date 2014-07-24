package io.divolte.server;

import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncomingRequestProcessor {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessor.class);
    
    private final LinkedBlockingQueue<HttpServerExchange> queue;

    public IncomingRequestProcessor() {
        this.queue = new LinkedBlockingQueue<HttpServerExchange>();
    }

    public void readQueue() {
        final int maxBatchSize = 100;
        final List<HttpServerExchange> batch = new ArrayList<>(maxBatchSize);
        
        while(true) {
            int batchSize = queue.drainTo(batch, maxBatchSize);
            batch.stream().forEach((exchange) -> {
                processExchange(exchange);
            });

            // if the batch was empty, block on the queue until something is available
            if (batchSize == 0) {
                batch.add(pollQuietly(queue, 1, TimeUnit.SECONDS));
            }
        }
    }
    
    private void processExchange(final HttpServerExchange exchange) {
        logger.debug("Handling exchange: {}", exchange);
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
