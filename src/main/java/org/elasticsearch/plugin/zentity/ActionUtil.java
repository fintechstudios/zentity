package org.elasticsearch.plugin.zentity;

import io.zentity.common.CompletableFutureUtil;
import io.zentity.model.ValidationException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * A utility class for actions that provides common handler functionality.
 */
public abstract class ActionUtil extends BaseRestHandler {
    /**
     * Wrap a consumer with error handling.
     *
     * @param consumer The consumer to wrap.
     * @return The wrapped consumer.
     */
    public static RestChannelConsumer errorHandlingConsumer(RestChannelConsumer consumer) {
        return channel -> {
            try {
                consumer.accept(channel);
            } catch (Throwable ex) {
                RestStatus status;
                Throwable unwrapped = CompletableFutureUtil.getCause(ex);

                if (unwrapped instanceof ElasticsearchStatusException) {
                    status = ((ElasticsearchStatusException) unwrapped).status();
                } else if (unwrapped instanceof ValidationException) {
                    // TODO: move validation handling to where the deserialization is done
                    status = RestStatus.BAD_REQUEST;
                } else {
                    status = RestStatus.INTERNAL_SERVER_ERROR;
                }

                if (!(unwrapped instanceof Exception)) {
                    unwrapped = new Exception(unwrapped);
                }

                channel.sendResponse(new BytesRestResponse(channel, status, (Exception) unwrapped));
            }
        };
    }
}
