package io.divolte.server.processing;

public interface ItemProcessor<E> {
    void process(E e);

    default void heartbeat() {
        // noop, override to implement heartbeats
    }

    default void cleanup() {
        // noop, override to implement cleanup
    }
}
