package io.zentity.common;

import java.util.function.Function;

public class FunctionalUtil {
    /**
     * A "fixed point combinator" interface for functional recursion.
     *
     * @see <a href="https://stackoverflow.com/a/35997193/4705719"></a>
     * @param <T> The input type.
     * @param <R> The result type.
     */
    @FunctionalInterface
    interface Recursable<T, R> extends Function<T, R> {
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
}
