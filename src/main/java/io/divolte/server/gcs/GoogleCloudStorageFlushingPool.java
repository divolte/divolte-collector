/*
 * Copyright 2014 GoDataDriven B.V.
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

package io.divolte.server.gcs;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.SchemaRegistry;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.processing.ProcessingPool;
import org.apache.avro.Schema;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;

@ParametersAreNonnullByDefault
public final class GoogleCloudStorageFlushingPool extends ProcessingPool<GoogleCloudStorageFlusher, AvroRecordBuffer>{
    public GoogleCloudStorageFlushingPool(final ValidatedConfiguration vc,
                                          final String name,
                                          final SchemaRegistry schemaRegistry) {
        this(vc,
             name,
             schemaRegistry.getSchemaBySinkName(name),
            1,
            10000);
    }

    public GoogleCloudStorageFlushingPool(final ValidatedConfiguration vc,
                                          final String name,
                                          final Schema schema,
                                          final int numThreads,
                                          final int maxQueueSize) {
        super(numThreads,
              maxQueueSize,
              String.format("Google Cloud Storage Flusher [%s]", Objects.requireNonNull(name)),
              () -> new GoogleCloudStorageFlusher(Objects.requireNonNull(vc), name, Objects.requireNonNull(schema)));
    }
}
