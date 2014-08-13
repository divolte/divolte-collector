package io.divolte.server;

import static io.divolte.server.ConcurrentUtils.*;
import io.divolte.server.hdfs.HdfsFlushingPool;
import io.divolte.server.kafka.KafkaFlushingPool;
import io.undertow.server.HttpServerExchange;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@ParametersAreNonnullByDefault
final class IncomingRequestProcessingPool {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessingPool.class);

    private final List<IncomingRequestProcessor> processors;

    public IncomingRequestProcessingPool() {
        this(ConfigFactory.load());
    }

    public IncomingRequestProcessingPool(final Config config) {
        final int numThreads = config.getInt("divolte.incoming_request_processor.threads");

        final ThreadGroup threadGroup = new ThreadGroup("Incoming Request Processing Pool");
        final ThreadFactory factory = createThreadFactory(threadGroup, "Incoming Request Processor - %d");
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads, factory);

        final Schema schema = schemaFromConfig(config);

        final KafkaFlushingPool kafkaFlushingPool = config.getBoolean("divolte.kafka_flusher.enabled") ? new KafkaFlushingPool(config) : null;
        final HdfsFlushingPool hdfsFlushingPool = config.getBoolean("divolte.hdfs_flusher.enabled") ? new HdfsFlushingPool(config, schema) : null;

        this.processors = Stream.generate(() -> new IncomingRequestProcessor(config, kafkaFlushingPool, hdfsFlushingPool, schema))
                           .limit(numThreads)
                           .collect(Collectors.toCollection(() -> new ArrayList<>(numThreads)));

        this.processors.forEach((processor) ->
            scheduleQueueReader(
                    executorService,
                    processor.getQueueReader())
        );
    }

    private Schema schemaFromConfig(final Config config) {
        try {
            final Parser parser = new Schema.Parser();
            if (config.hasPath("divolte.tracking.schema_file")) {
                final String schemaFileName = config.getString("divolte.tracking.schema_file");
                logger.info("Using Avro schema from configuration: {}", schemaFileName);
                return parser.parse(new File(schemaFileName));
            } else {
                logger.info("Using built in default Avro schema.");
                return parser.parse(Resources.toString(Resources.getResource("DefaultEventRecord.avsc"), StandardCharsets.UTF_8));
            }
        } catch(IOException ioe) {
            logger.error("Failed to load Avro schema file.");
            throw new RuntimeException("Failed to load Avro schema file.", ioe);
        }
    }

    public void enqueueIncomingExchangeForProcessing(final String partyId, final HttpServerExchange exchange) {
        processors.get((partyId.hashCode() & Integer.MAX_VALUE) % processors.size()).add(partyId, exchange);
    }
}
