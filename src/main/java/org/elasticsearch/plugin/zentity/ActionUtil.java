package org.elasticsearch.plugin.zentity;

import io.zentity.common.CompletableFutureUtil;
import io.zentity.model.ValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestStatus;

/**
 * A utility class for actions that provides common handler functionality.
 */
public abstract class ActionUtil extends BaseRestHandler {
    private static final Logger LOG = LogManager.getLogger(ActionUtil.class);

    /**
     * Wrap a consumer with error handling.
     *
     * @param consumer The consumer to wrap.
     * @return The wrapped consumer.
     */
    public static RestChannelConsumer errorHandlingConsumer(RestChannelConsumer consumer) {
        return (channel) -> {
            try {
                consumer.accept(channel);
            } catch (Throwable ex) {
                RestStatus status;
                Throwable unwrapped = CompletableFutureUtil.getCause(ex);

                if (unwrapped instanceof ElasticsearchException) {
                    status = ((ElasticsearchException) unwrapped).status();
                } else if (unwrapped instanceof ValidationException) {
                    // TODO: move validation handling to where the deserialization is done
                    status = RestStatus.BAD_REQUEST;
                } else {
                    LOG.error("Unknown failure", unwrapped);
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
