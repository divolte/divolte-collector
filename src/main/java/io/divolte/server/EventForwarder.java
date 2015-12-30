package io.divolte.server;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.Iterables;
import io.divolte.server.processing.Item;
import io.divolte.server.processing.ProcessingPool;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;

@ParametersAreNonnullByDefault
public abstract class EventForwarder<E> {
    public abstract void forward(final Item<E> event);

    private static EventForwarder<?> EMPTY_FORWARDER = new NoopEventForwarder<>();

    @ParametersAreNonnullByDefault
    private final static class NoopEventForwarder<E> extends EventForwarder<E> {
        @Override
        public void forward(final Item<E> event) {
            // Nothing to do.
        }
    }

    @ParametersAreNonnullByDefault
    private final static class SingleReceiverEventForwarder<E> extends EventForwarder<E> {
        private final ProcessingPool<?, E> receiver;

        private SingleReceiverEventForwarder(final ProcessingPool<?, E> receiver) {
            this.receiver = Objects.requireNonNull(receiver);
        }

        @Override
        public void forward(final Item<E> event) {
            receiver.enqueue(event);
        }
    }

    private final static class MultipleReceiverEventForwarder<E> extends EventForwarder<E> {
        private final ImmutableCollection<? extends ProcessingPool<?, E>> receivers;

        private MultipleReceiverEventForwarder(final ImmutableCollection<? extends ProcessingPool<?, E>> receivers) {
            this.receivers = Objects.requireNonNull(receivers);
        }

        @Override
        public void forward(final Item<E> event) {
            receivers.forEach(receiver -> receiver.enqueue(event));
        }
    }

    static <E> EventForwarder<E> create(final ImmutableCollection<? extends ProcessingPool<?, E>> receivers) {
        switch (receivers.size()) {
            case 0:
                @SuppressWarnings("unchecked")
                final EventForwarder<E> emptyForwarder = (EventForwarder<E>) EMPTY_FORWARDER;
                return emptyForwarder;
            case 1:
                return new SingleReceiverEventForwarder<>(Iterables.getOnlyElement(receivers));
            default:
                return new MultipleReceiverEventForwarder<>(receivers);
        }
    }
}
