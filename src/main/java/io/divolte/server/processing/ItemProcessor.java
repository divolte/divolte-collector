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

package io.divolte.server.processing;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;

import java.util.Queue;

public interface ItemProcessor<E> {
    ProcessingDirective process(Item<E> e);

    default ProcessingDirective process(final Queue<Item<E>> batch) {
        ProcessingDirective directive;
        do {
            // Note: processing should not throw an unchecked
            // exception unless no further processing should
            // take place.
            directive = process(batch.remove());
        } while (!batch.isEmpty() && directive == CONTINUE);
        return directive;
    }

    default ProcessingDirective heartbeat() {
        return CONTINUE;
    }

    default void cleanup() {
        // noop, override to implement cleanup
    }

    enum ProcessingDirective {
        CONTINUE,
        PAUSE
    }
}
