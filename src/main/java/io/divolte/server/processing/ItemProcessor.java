package io.divolte.server.processing;

import java.util.Iterator;
import java.util.List;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;

public interface ItemProcessor<E> {
    ProcessingDirective process(E e);

    default ProcessingDirective process(List<E> batch) {
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
