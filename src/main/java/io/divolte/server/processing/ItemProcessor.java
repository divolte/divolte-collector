package io.divolte.server.processing;

import java.util.Iterator;
import java.util.Queue;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;

public interface ItemProcessor<E> {
    ProcessingDirective process(E e);

    default ProcessingDirective process(final Queue<E> batch) {
        final Iterator<E> itr = batch.iterator();
        ProcessingDirective directive;
        do {
            directive = process(itr.next());
            itr.remove();
        } while (itr.hasNext() && directive == CONTINUE);
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
