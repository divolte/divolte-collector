package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.NoSuchElementException;

public enum KafkaSinkMode {
    NAKED, CONFLUENT;

    @JsonCreator
    public static KafkaSinkMode forValue(String value) {
        for (KafkaSinkMode mode : KafkaSinkMode.values()) {
            if (mode.toString().equals(value)) {
                return mode;
            }
        }
        throw new NoSuchElementException("Kafka sink mode " + value + " is not supported");
    }

    @JsonValue
    public String toString() {
        return name().toLowerCase();
    }
}
