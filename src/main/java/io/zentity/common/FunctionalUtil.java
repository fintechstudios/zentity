package io.zentity.common;

import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.CheckedSupplier;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public class FunctionalUtil {
    /**
     * Throw a checked exception as an unchecked one.
     *
     * <p>The compiler sees the signature with the throws T inferred to a RuntimeException type, so it
     * allows the unchecked exception to propagate.
     *
     * @see <a href="http://www.baeldung.com/java-sneaky-throws"></a>
     */
    @SuppressWarnings("unchecked")
    public static <E extends Throwable> void sneakyThrow(Throwable ex) throws E {
        throw (E) ex;
    }

    /**
     * Wrap a potentially erroneous function to throw checked exception as an unchecked one.
     *
     * @param supplier The function to wrap.
     * @param <T> The return type.
     * @param <E> The error type.
     * @return The result, if no error occurs.
     */
    public static <T, E extends Exception> T sneakyWrap(CheckedSupplier<T, E> supplier) {
        try {
            return supplier.get();
        } catch (Exception ex) {
            sneakyThrow(ex);
            return null;
        }
    }

    /**
     * A "fixed point combinator" interface for functional recursion.
     *
     * @see <a href="https://stackoverflow.com/a/35997193/4705719"></a>
     * @param <T> The input type.
     * @param <R> The result type.
     */
    @FunctionalInterface
    public interface Recursable<T, R> extends Function<T, R> {
        /**
         * The recursive call handler.
         *
         * @param input The input.
         * @param func An instance of the function for recursing.
         * @return The result of the recursion.
         */
        R recurse(T input, Recursable<T, R> func);

        /**
         * An {@link Function#apply} implementation that kicks off recursion.
         *
         * @param input The initial input.
         * @return The result of the recursion.
         */
        default R apply(T input) {
            return recurse(input, this);
        }
    }

    @FunctionalInterface
    public interface UnCheckedFunction<T, R, E extends Exception> extends Function<T, R> {
        R applyThrows(T input) throws E;

        default R apply(T input) {
            return sneakyWrap(() -> this.applyThrows(input));
        }

        static <T, R, E extends Exception> UnCheckedFunction<T, R, E> from(CheckedFunction<T, R, E> f) {
            return f::apply;
        }
    }

    @FunctionalInterface
    public interface UnCheckedUnaryOperator<T, E extends Exception> extends UnaryOperator<T> {
        T applyThrows(T input) throws E;

        default T apply(T input) {
            return sneakyWrap(() -> this.applyThrows(input));
        }

        static <T, E extends Exception> UnCheckedUnaryOperator<T, E> from(CheckedFunction<T, T, E> f) {
            return f::apply;
        }
    }

    @FunctionalInterface
    public interface UnCheckedConsumer<T, E extends Exception> extends Consumer<T> {
        void acceptThrows(T input) throws E;

        default void accept(T input) {
            sneakyWrap(() -> {
                this.acceptThrows(input);
                return null;
            });
        }

        static <T, E extends Exception> UnCheckedConsumer<T, E> from(CheckedConsumer<T, E> f) {
            return f::accept;
        }
    }


    @FunctionalInterface
    public interface UnCheckedSupplier<T, E extends Exception> extends Supplier<T> {
        T getThrows() throws E;

        default T get() {
            return sneakyWrap(this::getThrows);
        }

        static <T, E extends Exception> UnCheckedSupplier<T, E> from(CheckedSupplier<T, E> f) {
            return f::get;
        }
    }

    @FunctionalInterface
    public interface UnCheckedBiFunction<T, U, R, E extends Exception> extends BiFunction<T, U, R> {
        R applyThrows(T var1, U var2) throws E;

        default R apply(T var1, U var2) {
            return sneakyWrap(() -> this.applyThrows(var1, var2));
        }

        static <T, U, R, E extends Exception> UnCheckedBiFunction<T, U, R, E> from(CheckedBiFunction<T, U, R, E> f) {
            return f::apply;
        }
    }

    @FunctionalInterface
    public interface UnCheckedRunnable<E extends Exception> extends Runnable {
        void runThrows() throws E;

        default void run() {
            sneakyWrap(() -> {
                this.runThrows();
                return null;
            });
        }

        static <E extends Exception> UnCheckedRunnable<E> from(CheckedRunnable<E> runnable) {
            return runnable::run;
        }
    }
}
