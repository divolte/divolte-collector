package io.divolte.server.processing;

import java.util.Queue;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;

public interface ItemProcessor<E> {
    ProcessingDirective process(E e);

    default ProcessingDirective process(final Queue<E> batch) {
        ProcessingDirective directive;
        do {
            // Note: processing should not throw an unchecked
            // exception unless no further processing should
            // take place.
            directive = process(batch.remove());
        } while (batch.isEmpty() && directive == CONTINUE);
        return directive;
    }

    default ProcessingDirective heartbeat() {
        return CONTINUE;
    }

    default void cleanup() {
        // noop, override to implement cleanup
    }

    public enum ProcessingDirective {
        CONTINUE,
        PAUSE
    }
}
