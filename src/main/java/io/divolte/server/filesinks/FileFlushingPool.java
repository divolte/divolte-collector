/*
 * Copyright 2018 GoDataDriven B.V.
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

package io.divolte.server.filesinks;

import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.config.FileSinkConfiguration;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.processing.ProcessingPool;

@ParametersAreNonnullByDefault
public class FileFlushingPool extends ProcessingPool<FileFlusher, AvroRecordBuffer> {

    public FileFlushingPool(
            final ValidatedConfiguration vc,
            final String sinkName,
            final int numThreads,
            final int maxQueueSize,
            final FileManager.FileManagerFactory factory) {
        super(numThreads,
              maxQueueSize,
              String.format(
                      "%s Flusher [%s]",
                      vc.configuration().getSinkConfiguration(sinkName, FileSinkConfiguration.class).getReadableType(),
                      Objects.requireNonNull(sinkName)),
              () -> new FileFlusher(
                      vc.configuration().getSinkConfiguration(sinkName, FileSinkConfiguration.class).fileStrategy,
                      factory.create())
              );
    }
}
