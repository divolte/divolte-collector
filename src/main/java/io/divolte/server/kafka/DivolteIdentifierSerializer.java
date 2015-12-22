/*
 * Copyright 2015 GoDataDriven B.V.
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

package io.divolte.server.kafka;

import io.divolte.server.DivolteIdentifier;
import org.apache.kafka.common.serialization.Serializer;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@ParametersAreNonnullByDefault
class DivolteIdentifierSerializer implements Serializer<DivolteIdentifier> {
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Nothing needed here.
    }

    @Override
    public byte[] serialize(final String topic, final DivolteIdentifier data) {
        return data.value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        // Nothing needed here.
    }
}
