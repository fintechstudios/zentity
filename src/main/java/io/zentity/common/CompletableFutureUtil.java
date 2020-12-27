package io.zentity.common;

import org.elasticsearch.common.CheckedFunction;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class CompletableFutureUtil {
    /**
     * Wrap a checked function's error in a {@link CompletionException}.
     *
     * @param func The checked function.
     * @param <T> The type of input.
     * @return The error-wrapped function.
     */
    public static <T, R> Function<T, R> uncheckedFunction(CheckedFunction<T, R, ?> func) {
        return (val) -> {
            try {
                return func.apply(val);
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        };
    }

    /**
     * Returns a new {@link CompletionStage} that, when the passed stage completes exceptionally,
     * is composed using the results of the supplied function applied to this stage's exception.
     *
     * @param stage The current future stage.
     * @param fn    The handler function.
     * @param <T>   The future result type.
     * @return The wrapped future stage.
     * @see <a href="https://docs.oracle.com/en/java/javase/12/docs/api/java.base/java/util/concurrent/CompletionStage.html#exceptionallyCompose(java.util.function.Function)"></a>
     * @see <a href="https://github.com/spotify/completable-futures/blob/25bbd6e0c1c6cef974112aeb859938a6e927f4c5/src/main/java/com/spotify/futures/CompletableFutures.java"></a>
     */
    public static <T> CompletionStage<T> composeExceptionally(CompletionStage<T> stage, Function<Throwable, ? extends CompletableFuture<T>> fn) {
        return stage
            .thenApply(CompletableFuture::completedFuture)
            .exceptionally(fn)
            .thenCompose(Function.identity());
    }

    /**
     * Returns a new {@link CompletableFuture} that, when the passed future completes exceptionally, is
     * composed using the results of the supplied function applied to this stage's exception.
     *
     * @param fut The current future.
     * @param fn  The handler function.
     * @param <T> The future result type.
     * @return The wrapped future.
     * @see <a href="https://docs.oracle.com/en/java/javase/12/docs/api/java.base/java/util/concurrent/CompletionStage.html#exceptionallyCompose(java.util.function.Function)"></a>
     * @see <a href="https://github.com/spotify/completable-futures/blob/25bbd6e0c1c6cef974112aeb859938a6e927f4c5/src/main/java/com/spotify/futures/CompletableFutures.java"></a>
     */
    public static <T> CompletableFuture<T> composeExceptionally(CompletableFuture<T> fut, Function<Throwable, ? extends CompletableFuture<T>> fn) {
        return (CompletableFuture<T>) composeExceptionally((CompletionStage<T>) fut, fn);
    }

    /**
     * Unwrap a {@link CompletionException} to get the first cause that is not a {@link CompletionException}, or the
     * last-most {@link CompletionException} if there is no more specific cause.
     *
     * @param ex The exception.
     * @return The most specific {@link Throwable} that was found.
     */
    public static Throwable getCause(Throwable ex) {
        Objects.requireNonNull(ex, "exception cannot be null");
        if ((ex instanceof CompletionException || ex instanceof ExecutionException) && ex.getCause() != null) {
            return getCause(ex.getCause());
        }

        return ex;
    }

    /**
     * Like {@link CompletableFuture#completedFuture} but completed exceptionally.
     *
     * @param throwable the cause.
     * @param <T>       the future type.
     * @return the future, completed exceptionally.
     */
    public static <T> CompletableFuture<T> exceptionallyCompletedFuture(Throwable throwable) {
        CompletableFuture<T> fut = new CompletableFuture<>();
        fut.completeExceptionally(throwable);
        return fut;
    }
}
