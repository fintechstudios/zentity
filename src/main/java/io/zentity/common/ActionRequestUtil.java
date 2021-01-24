package io.zentity.common;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class ActionRequestUtil {
    /**
     * Wrap an {@link ActionRequestBuilder} in a {@link CompletableFuture}.
     *
     * @param reqBuilder The request builder.
     * @param executor   The executor to supply async context.
     * @param <T>        The response type.
     * @return A completable future that resolves with the response.
     * @see <a href="https://mincong.io/2020/07/26/es-client-completablefuture/"></a>
     */
    public static <T extends ActionResponse> CompletableFuture<T> toCompletableFuture(ActionRequestBuilder<?, T> reqBuilder, Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            System.out.println("Thread.currentThread().getName() = " + Thread.currentThread().getName());
            return reqBuilder.get();
        }, executor);
    }
}
