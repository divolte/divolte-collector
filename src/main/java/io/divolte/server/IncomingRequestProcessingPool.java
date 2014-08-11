package io.divolte.server;

import io.divolte.server.kafka.KafkaFlushingPool;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import static io.divolte.server.ConcurrentUtils.*;

final class IncomingRequestProcessingPool {
    private final List<IncomingRequestProcessor> processors;

    public IncomingRequestProcessingPool() {
        this(ConfigFactory.load());
    }

    public IncomingRequestProcessingPool(final Config config) {
        final int numThreads = config.getInt("divolte.incoming_request_processor.threads");

        final ThreadGroup threadGroup = new ThreadGroup("Incoming Request Processing Pool");
        final ThreadFactory factory = createThreadFactory(threadGroup, "Incoming Request Processor - %d");
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads, factory);

        final KafkaFlushingPool kafkaFlushingPool = new KafkaFlushingPool(config);
        final HdfsFlushingPool hdfsFlushingPool = new HdfsFlushingPool(config);

        processors = Stream.generate(() -> new IncomingRequestProcessor(kafkaFlushingPool, hdfsFlushingPool))
                           .limit(numThreads)
                           .collect(Collectors.toCollection(() -> new ArrayList<>(numThreads)));

        processors.forEach((processor) ->
            scheduleQueueReader(
                    executorService,
                    processor.getQueueReader())
        );
    }

    public void enqueueIncomingExchangeForProcessing(final String partyId, final HttpServerExchange exchange) {
        processors.get((partyId.hashCode() & Integer.MAX_VALUE) % processors.size()).add(partyId, exchange);
    }
}
