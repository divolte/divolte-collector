package io.divolte.server;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.*;

public abstract class OptionalConfig<T> {

    private OptionalConfig() {
        // Prevent external extension.
    }

    private static final OptionalConfig<?> EMPTY = new ConfigAbsent();

    //Constructors
    public static <T> OptionalConfig<T> absent(String message) {
        return new ConfigAbsent<>(message);
    }

    public static <T> OptionalConfig<T> absent(String message, Exception e) {
        return new ConfigAbsent<>(message, e);
    }

    public static <T> OptionalConfig<T> absent(Exception e) {
        return new ConfigAbsent<>(e);
    }

    public static <T> OptionalConfig<T> present(T value) {
        return new ConfigPresent<>(value);
    }

    public static <T> OptionalConfig<T> of(T value) { return present(value); }

    public static <T> OptionalConfig<T> of(Exception exception) { return absent(exception); }

    public static <T> OptionalConfig<T> of(Function<String, ? extends T> func, String value ) {
        Objects.requireNonNull(func);
        try {
            return OptionalConfig.of(func.apply(value));
        } catch (Exception ex) {
            return OptionalConfig.of(ex);
        }
    }

    public static <T> OptionalConfig<T> of(BiFunction<String, TimeUnit, ? extends T> func, String value, TimeUnit unit ) {
        Objects.requireNonNull(func);
        try {
            return OptionalConfig.of(func.apply(value, unit));
        } catch (Exception ex) {
            return OptionalConfig.of(ex);
        }
    }

    public static <T> OptionalConfig<T> ofNullable(T value) {
        return value == null ? empty() : of(value);
    }

    public static<T> OptionalConfig<T> empty() {
        @SuppressWarnings("unchecked")
        OptionalConfig<T> t = (OptionalConfig<T>) EMPTY;
        return t;
    }


    // abstract methods
    public abstract boolean isPresent();

    public abstract boolean isAbsent();

    public abstract void throwException();

    //Optional or Collection like api
    public abstract OptionalConfig<T> filter(Predicate<? super T> predicate);

    public abstract <U> OptionalConfig<U> flatMap(Function<? super T,OptionalConfig<U>> mapper);

    public abstract T get();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    public abstract void ifPresent(Consumer<? super T> consumer);

    public abstract <U> OptionalConfig<U> map(Function<? super T,? extends U> mapper);

    public abstract T orElse(T other);

    public abstract T orElseGet(Supplier<? extends T> other);

    public abstract <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws X;

    //Non abstract subclasses
    protected final static class ConfigAbsent<T> extends OptionalConfig<T> {

        private RuntimeException exception;

        public ConfigAbsent() {
            this.exception = null;
        }

        public ConfigAbsent(String message) {
            this.exception = new IllegalStateException(message);
        }

        public ConfigAbsent(String message, Exception e) {
            this.exception = new IllegalStateException(message, e);
        }

        public ConfigAbsent(Exception e) {
            this.exception = new IllegalStateException(e);
        }

        @Override
        public boolean isPresent() {
            return false;
        }

        @Override
        public boolean isAbsent() {
            return true;
        }

        @Override
        public void ifPresent(Consumer<? super T> consumer) {
            //Do nothing
        }

        @Override
        public <U> OptionalConfig<U> map(Function<? super T, ? extends U> mapper) {
            return empty();
        }

        @Override
        public T orElse(T other) {
            return other;
        }

        @Override
        public T orElseGet(Supplier<? extends T> other) {
            return other.get();
        }

        @Override
        public <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws X {
            throw exceptionSupplier.get();
        }

        @Override
        public T get() {
            throw new NoSuchElementException("No value present");
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.exception);
        }

        @Override
        public boolean equals(final Object other) {
            return this == other ||
                   null != other && getClass() == other.getClass() && Objects.equals(exception, ((ConfigAbsent)other).exception);
        }

        @Override
        public void throwException() {
            throw this.exception;
        }

        @Override
        public OptionalConfig<T> filter(Predicate<? super T> predicate) {
            return this;
        }

        @Override
        public <U> OptionalConfig<U> flatMap(Function<? super T, OptionalConfig<U>> mapper) {
            return empty();
        }

        @Override
        public String toString() {
            return this.exception != null
                    ? String.format("ConfigAbsent[%s]", this.exception.getMessage())
                    : "OptionalConfig.empty";
        }

    }

    protected final static class ConfigPresent<T> extends OptionalConfig<T> {

        private T value;

        public ConfigPresent(T value) {
            this.value = value;
        }

        @Override
        public boolean isPresent() {
            return true;
        }

        @Override
        public boolean isAbsent() {
            return false;
        }

        @Override
        public void ifPresent(Consumer<? super T> consumer) {
            if (isPresent())
                consumer.accept(this.value);
        }

        @Override
        public <U> OptionalConfig<U> map(Function<? super T, ? extends U> mapper) {
            Objects.requireNonNull(mapper);
            if (!isPresent())
                return empty();
            else {
                return OptionalConfig.ofNullable(mapper.apply(value));
            }
        }

        @Override
        public T orElse(T other) {
            return value != null ? value : other;
        }

        @Override
        public T orElseGet(Supplier<? extends T> other) {
            return value != null ? value : other.get();
        }

        @Override
        public <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws X {
            if (value != null) {
                return value;
            } else {
                throw exceptionSupplier.get();
            }

        }

        @Override
        public T get() {
            if (value == null) {
                throw new NoSuchElementException("No value present");
            }
            return this.value;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.value);
        }

        @Override
        public boolean equals(final Object other) {
            return this == other ||
                   null != other && getClass() == other.getClass() && Objects.equals(value, ((ConfigPresent)other).value);
        }

        @Override
        public void throwException() {
            //log.error("Method throwException() called on a ConfigPresent instance");
        }

        @Override
        public OptionalConfig<T> filter(Predicate<? super T> predicate) {
            Objects.requireNonNull(predicate);
            if (!isPresent())
                return this;
            else
                return predicate.test(value) ? this : empty();
        }

        @Override
        public <U> OptionalConfig<U> flatMap(Function<? super T, OptionalConfig<U>> mapper) {
            Objects.requireNonNull(mapper);
            if (!isPresent())
                return empty();
            else {
                return Objects.requireNonNull(mapper.apply(value));
            }
        }

        @Override
        public String toString() {
            return this.value != null
                    ? String.format("ConfigPresent[%s]", this.value)
                    : "OptionalConfig.empty";
        }

    }
}
