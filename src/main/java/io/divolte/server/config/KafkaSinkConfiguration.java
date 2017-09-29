package io.divolte.server.config;

import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteIdentifier;
import io.divolte.server.kafka.KafkaFlushingPool;
import io.divolte.server.kafka.Serializers;
import org.apache.kafka.clients.producer.KafkaProducer;

@ParametersAreNonnullByDefault
public class KafkaSinkConfiguration extends SinkConfiguration {
    private static final String DEFAULT_TOPIC = "divolte";
    private static final KafkaSinkMode DEFAULT_SINK_MODE = KafkaSinkMode.NAKED;

    public final String topic;
    public final KafkaSinkMode mode;

    @JsonCreator
    @ParametersAreNullableByDefault
    KafkaSinkConfiguration(@JsonProperty(defaultValue=DEFAULT_TOPIC) final String topic, @JsonProperty final KafkaSinkMode mode) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        this.topic = Optional.ofNullable(topic).orElse(DEFAULT_TOPIC);
        this.mode = Optional.ofNullable(mode).orElse(DEFAULT_SINK_MODE);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
            .add("topic", topic)
            .add("mode", mode);
    }

    @Override
    public SinkFactory getFactory() {
        return (vc, sink, registry) -> {
            final KafkaProducer<DivolteIdentifier, AvroRecordBuffer> producer =
                new KafkaProducer<>(vc.configuration().global.kafka.producer,
                                    Serializers.createKeySerializer(),
                                    mode.serializerFactory.apply(registry.getSchemaBySinkName(sink)));
            return new KafkaFlushingPool(sink,
                                         vc.configuration().global.kafka.threads,
                                         vc.configuration().global.kafka.bufferSize,
                                         topic,
                                         producer
            );
        };
    }
}
