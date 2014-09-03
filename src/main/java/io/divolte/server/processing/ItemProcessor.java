package io.divolte.server.processing;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;

public interface ItemProcessor<E> {
    ProcessingDirective process(E e);

    default ProcessingDirective heartbeat() {
        return CONTINUE;
    }

    default void cleanup() {
        // noop, override to implement cleanup
    }

    public enum ProcessingDirective {
        CONTINUE,
        PAUSE;
    }
}
