package io.divolte.server;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
final class LazyReference<T> {
    @Nonnull
    private final Supplier<T> generator;

    @Nonnull
    private Optional<T> reference = Optional.empty();

    public LazyReference(@Nonnull final Supplier<T> generator) {
        this.generator = Objects.requireNonNull(generator);
    }

    public T get() {
        T result;
        if (reference.isPresent()) {
            result = reference.get();
        } else {
            result = generator.get();
            reference = Optional.of(result);
        }
        return result;
    }
}
