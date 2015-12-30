/*
 * Copyright 2015 GoDataDriven B.V.
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
