package io.divolte.server;

import static io.divolte.server.ConcurrentUtils.createThreadFactory;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

final class IncomingRequestProcessingPool {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessingPool.class);
    private final List<IncomingRequestProcessor> processors;

    public IncomingRequestProcessingPool() {
        this(ConfigFactory.load());
    }

    public IncomingRequestProcessingPool(final Config config) {
        final int numSerializationThreads = config.getInt("divolte.incoming_request_processor.threads");

        final ThreadGroup threadGroup = new ThreadGroup("Incoming Request Processing Pool");
        final ThreadFactory factory = createThreadFactory(threadGroup, "Incoming Request Processor - %d");
        final ExecutorService executorService = Executors.newFixedThreadPool(numSerializationThreads, factory);

        final LocalFileFlushingPool localFlushingPool = new LocalFileFlushingPool(config);

        processors = Stream.generate(() -> new IncomingRequestProcessor(localFlushingPool))
        .limit(numSerializationThreads)
        .collect(Collectors.toCollection(() -> new ArrayList<>(numSerializationThreads)));

        processors.forEach((processor) -> {
            scheduleQueueReader(
                    executorService,
                    processor);
        });
    }

    private void scheduleQueueReader(final ExecutorService es, final IncomingRequestProcessor processor) {
        CompletableFuture
        .runAsync(processor.getQueueReader(), es)
        .whenComplete((voidValue, error) -> {
            // In case the reader for some reason escapes its loop with an exception,
            // log any uncaught exceptions and reschedule
            if (error != null) {
                logger.warn("Uncaught exception in incoming queue reader thread.", error);
                scheduleQueueReader(es, processor);
            }
        });
    }

    public void enqueueIncomingExchangeForProcessing(final String partyId, final HttpServerExchange exchange) {
        // we assign requests with the same party ID to the same thread,
        // such that we do not re-order messages for the same party ID.
        processors.get((partyId.hashCode() & Integer.MAX_VALUE) % processors.size()).add(partyId, exchange);
    }
}
