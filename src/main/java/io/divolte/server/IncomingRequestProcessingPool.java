package io.divolte.server;

import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class IncomingRequestProcessingPool {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessingPool.class);
    private final List<IncomingRequestProcessor> processors;
    
    public IncomingRequestProcessingPool() {
        this(ConfigFactory.load());
    }
    
    public IncomingRequestProcessingPool(Config config) {
        final int numSerializationThreads = config.getInt("divolte.server.serialization.threads");
        
        processors = new ArrayList<IncomingRequestProcessor>(numSerializationThreads);
        Stream.generate(() -> new LinkedBlockingQueue<HttpServerExchange>())
        .limit(numSerializationThreads)
        .forEach((queue) -> {
            final IncomingRequestProcessor processor = new IncomingRequestProcessor();
            processors.add(processor);
            scheduleQueueReader(
                    Executors.newFixedThreadPool(
                            1,
                            (runnable) -> new Thread(runnable, "Incoming HttpServerExchange reader thread for queue " + queue.hashCode())),
                    processor);
        });
    }
    
    private void scheduleQueueReader(final ExecutorService es, final IncomingRequestProcessor queueReader) {
        CompletableFuture
        .runAsync(() -> { queueReader.readQueue(); }, es)
        .whenComplete((voidValue, error) -> {
            // In case the reader for some reason escapes its loop with an exception,
            // log any uncaught exceptions and reschedule
            if (error != null) {
                logger.warn("Uncaught exception in incoming queue reader thread.", error);
                scheduleQueueReader(es, queueReader);
            }
        });
    }
    
    public void enqueueIncomingExchangeForProcessing(String partyId, HttpServerExchange exchange) {
        processors.get((partyId.hashCode() & Integer.MAX_VALUE) % processors.size()).add(exchange);
    }
}
