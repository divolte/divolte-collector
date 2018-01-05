/*
 * Copyright 2018 GoDataDriven B.V.
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
