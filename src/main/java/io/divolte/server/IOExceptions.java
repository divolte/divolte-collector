package io.divolte.server;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Some utility methods for working with methods that throw {@link IOException}.
 *
 * Functional interfaces often prohibit checked exceptions, which makes them
 * difficult to use with IO-related methods that throw {@linkplain IOException}.
 * This class contains methods that can be used to wrap such methods, tunneling
 * {@link IOException} as {@link UncheckedIOException} when it is thrown.
 */
public final class IOExceptions {
    private IOExceptions() {
        // Prevent external instantiation.
    }

    @FunctionalInterface
    public interface IOFunction<T, R> {
        R apply(T t) throws IOException;
    }

    public static <T, R> Function<T, R> wrap(final IOFunction<T, R> f) {
        return x -> {
            try {
                return f.apply(x);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    @FunctionalInterface
    public interface IOSupplier<R> {
        R get() throws IOException;
    }

    public static <R> Supplier<R> wrap(final IOSupplier<R> f) {
        return () -> {
            try {
                return f.get();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
