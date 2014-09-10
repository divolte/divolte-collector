package io.divolte.server.kafka;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.processing.ItemProcessor;
import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.stream.Collectors;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;

@ParametersAreNonnullByDefault
@NotThreadSafe
final class KafkaFlusher implements ItemProcessor<AvroRecordBuffer> {
    private final static Logger logger = LoggerFactory.getLogger(KafkaFlusher.class);

    private final String topic;
    private final Producer<byte[], byte[]> producer;

    // On failure, we pause delivery and store the failed operation here.
    // During heartbeats it will be retried until success.
    private Optional<KafkaSender> pendingOperation = Optional.empty();

    public KafkaFlusher(final Config config) {
        Objects.requireNonNull(config);
        final ProducerConfig producerConfig = new ProducerConfig(getProperties(config, "divolte.kafka_flusher.producer"));
        topic = config.getString("divolte.kafka_flusher.topic");
        producer = new Producer<>(producerConfig);
    }

    @Override
    public ProcessingDirective process(AvroRecordBuffer record) {
        logger.debug("Processing individual record.", record);
        return send(() -> {
            producer.send(buildMessage(record));
            logger.debug("Sent individual record to Kafka.", record);
        });
    }

    @Override
    public ProcessingDirective process(final Queue<AvroRecordBuffer> batch) {
        final int batchSize = batch.size();
        final ProcessingDirective result;
        switch (batchSize) {
        case 0:
            logger.warn("Ignoring empty batch of events.");
            result = CONTINUE;
            break;
        case 1:
            result = process(batch.remove());
            break;
        default:
            logger.debug("Processing batch of {} records.", batchSize);
            final List<KeyedMessage<byte[], byte[]>> kafkaMessages =
                    batch.stream()
                         .map(this::buildMessage)
                         .collect(Collectors.toCollection(() -> new ArrayList<>(batchSize)));
            // Clear the messages now; on failure they'll be retried as part of our
            // pending operation.
            batch.clear();
            result = send(() -> {
                producer.send(kafkaMessages);
                logger.debug("Sent {} records to Kafka.", batchSize);
            });
         }
        return result;
    }

    @Override
    public ProcessingDirective heartbeat() {
        return pendingOperation.map((t) -> {
            logger.debug("Retrying to send message(s) that failed.");
            return send(t);
        }).orElse(CONTINUE);
    }

    @FunctionalInterface
    private interface KafkaSender {
        public abstract void send() throws FailedToSendMessageException;
    }

    private ProcessingDirective send(final KafkaSender sender) {
        ProcessingDirective result;
        try {
            sender.send();
            pendingOperation = Optional.empty();
            result = CONTINUE;
        } catch (final FailedToSendMessageException e) {
            logger.warn("Failed to send message(s) to Kafka! (Will retry.)", e);
            pendingOperation = Optional.of(sender);
            result = PAUSE;
        }
        return result;
    }

    private KeyedMessage<byte[], byte[]> buildMessage(final AvroRecordBuffer record) {
        // Extract the AVRO record as a byte array.
        // (There's no way to do this without copying the array.)
        final ByteBuffer avroBuffer = record.getByteBuffer();
        final byte[] avroBytes = new byte[avroBuffer.remaining()];
        avroBuffer.get(avroBytes);
        return new KeyedMessage<>(topic, record.getPartyId().value.getBytes(StandardCharsets.UTF_8), avroBytes);
    }

    private static final Joiner COMMA_JOINER = Joiner.on(',');

    private static Properties getProperties(final Config config, final String path) {
        final Properties properties = new Properties();
        for (final Map.Entry<String,ConfigValue> entry : config.getConfig(path).entrySet()) {
            final ConfigValue configValue = entry.getValue();
            final String value;
            switch (configValue.valueType()) {
                case STRING:
                case BOOLEAN:
                case NUMBER:
                    value = configValue.unwrapped().toString();
                    break;
                case LIST:
                    final ConfigList configList = (ConfigList)configValue;
                    // We only need to support 'simple' types here.
                    value = COMMA_JOINER.join(configList.unwrapped());
                    break;
                case OBJECT:
                case NULL:
                default:
                    throw new IllegalStateException("Property type not supported for Kafka configuration: " + entry);
            }
            properties.setProperty(entry.getKey(), value);
        }
        return properties;
    }
}
