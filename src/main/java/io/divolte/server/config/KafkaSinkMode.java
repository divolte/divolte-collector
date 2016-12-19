/*
 * Copyright 2016 GoDataDriven B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
