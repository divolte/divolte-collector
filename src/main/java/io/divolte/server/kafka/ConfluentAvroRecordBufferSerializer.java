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

import io.divolte.server.AvroRecordBuffer;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.ByteBuffer;
import java.util.Map;

@ParametersAreNonnullByDefault
class ConfluentAvroRecordBufferSerializer extends ConfluentDivolteSerializer<AvroRecordBuffer> {

    public ConfluentAvroRecordBufferSerializer(int schemaId) {
        super(schemaId);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Nothing to do.
    }

    @Override
    protected ByteBuffer serializeData(final AvroRecordBuffer data) {
        return data.getByteBuffer();
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}
