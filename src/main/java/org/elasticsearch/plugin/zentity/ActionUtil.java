package org.elasticsearch.plugin.zentity;

import io.zentity.common.CompletableFutureUtil;
import io.zentity.model.ValidationException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.function.Function;

/**
 * A utility class for actions that provides common handler functionality.
 */
public abstract class ActionUtil extends BaseRestHandler {
    public static Function<Throwable, Void> channelErrorHandler(RestChannel channel) {
        return (ex) -> {
            RestStatus status;
            Throwable unwrapped = CompletableFutureUtil.getCause(ex);

            if (unwrapped instanceof ElasticsearchException) {
                status = ((ElasticsearchException) unwrapped).status();
            } else if (unwrapped instanceof ValidationException) {
                // TODO: move validation handling to where the deserialization is done
                status = RestStatus.BAD_REQUEST;
            } else {
                status = RestStatus.INTERNAL_SERVER_ERROR;
            }

            if (!(unwrapped instanceof Exception)) {
                unwrapped = new Exception(unwrapped);
            }

            RestResponse response;
            try {
                response = new BytesRestResponse(channel, status, (Exception) unwrapped);
            } catch (IOException ioEx) {
                response = new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Could not serialize exception: " + ioEx);
            }

            channel.sendResponse(response);
            return null;
        };
    };

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
                channelErrorHandler(channel).apply(ex);
            }
        };
    }
}
