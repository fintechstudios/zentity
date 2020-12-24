package io.zentity.common;

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedSupplier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class CompletableFutureUtil {
    public static <T> Consumer<T> checkedConsumer(CheckedConsumer<T, ?> consumer) {
        return (val) -> {
            try {
                consumer.accept(val);
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        };
    }

    public static <T> Supplier<T> checkedSupplier(CheckedSupplier<T, ?> supplier) {
        return () -> {
            try {
                return supplier.get();
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        };
    }

    /**
     * Returns a new CompletionStage that, when this stage completes exceptionally, is composed using the results of the supplied function applied to this stage's exception.
     *
     * @param stage the current future stage
     * @param fn    the handler function
     * @param <T>   the future result type
     * @return the wrapped future stage
     * @see <a href="https://docs.oracle.com/en/java/javase/12/docs/api/java.base/java/util/concurrent/CompletionStage.html#exceptionallyCompose(java.util.function.Function)"></a>
     * @see <a href="https://github.com/spotify/completable-futures/blob/25bbd6e0c1c6cef974112aeb859938a6e927f4c5/src/main/java/com/spotify/futures/CompletableFutures.java"></a>
     */
    public static <T> CompletionStage<T> composeExceptionally(CompletionStage<T> stage, Function<Throwable, ? extends CompletableFuture<T>> fn) {
        return stage
            .thenApply(CompletableFuture::completedFuture)
            .exceptionally(fn)
            .thenCompose(Function.identity());
    }
}
