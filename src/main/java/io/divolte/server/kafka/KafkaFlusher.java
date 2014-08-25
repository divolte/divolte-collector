package io.divolte.server.kafka;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.processing.ItemProcessor;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;

@ParametersAreNonnullByDefault
@NotThreadSafe
final class KafkaFlusher implements ItemProcessor<AvroRecordBuffer> {
    private final String topic;
    private final Producer<byte[], byte[]> producer;

    public KafkaFlusher(final Config config) {
        Objects.requireNonNull(config);
        final ProducerConfig producerConfig = new ProducerConfig(getProperties(config, "divolte.kafka_flusher.producer"));
        topic = config.getString("divolte.kafka_flusher.topic");
        producer = new Producer<>(producerConfig);
    }

    @Override
    public void process(AvroRecordBuffer record) {
        producer.send(buildMessage(record));
    }

    private KeyedMessage<byte[], byte[]> buildMessage(final AvroRecordBuffer record) {
        // Extract the AVRO record as a byte array.
        // (There's no way to do this without copying the array.)
        final ByteBuffer avroBuffer = record.getByteBuffer();
        final byte[] avroBytes = new byte[avroBuffer.remaining()];
        avroBuffer.get(avroBytes);
        return new KeyedMessage<>(topic, record.getPartyId().getValue().getBytes(StandardCharsets.UTF_8), avroBytes);
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
