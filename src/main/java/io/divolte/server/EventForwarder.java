package io.divolte.server;

import com.google.common.collect.ImmutableList;
import io.divolte.server.processing.Item;
import io.divolte.server.processing.ProcessingPool;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;

@ParametersAreNonnullByDefault
public final class EventForwarder<E> {
    private final ImmutableList<ProcessingPool<?, E>> receivers;

    public EventForwarder(final ImmutableList<ProcessingPool<?, E>> receivers) {
        this.receivers = Objects.requireNonNull(receivers);
    }

    public void forward(final Item<E> event) {
        receivers.forEach(receiver -> receiver.enqueue(event));
    }
}
