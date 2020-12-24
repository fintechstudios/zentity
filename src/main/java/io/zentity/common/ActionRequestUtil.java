package io.zentity.common;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;

import java.util.concurrent.CompletableFuture;

import static io.zentity.common.CompletableFutureUtil.checkedSupplier;

public class ActionRequestUtil {
    /**
     * Wrap an {@link ActionRequestBuilder} in a {@link CompletableFuture}.
     *
     * @param reqBuilder The request builder.
     * @param <T> The response type.
     * @return A completable future that executes the request.
     */
    public static <T extends ActionResponse> CompletableFuture<T> toCompletableFuture(ActionRequestBuilder<?, T> reqBuilder) {
        return CompletableFuture.supplyAsync(checkedSupplier(reqBuilder::get));
    }
}
