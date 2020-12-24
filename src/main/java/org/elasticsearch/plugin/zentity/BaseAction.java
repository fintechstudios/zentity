package org.elasticsearch.plugin.zentity;

import io.zentity.model.ValidationException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestStatus;

/**
 * A base class for actions that provides common handler functionality.
 */
public abstract class BaseAction extends BaseRestHandler {
    /**
     * Wrap a consumer with error handling.
     *
     * @param consumer The consumer to wrap.
     * @return The wrapped consumer.
     */
    public static RestChannelConsumer errorHandlingConsumer(RestChannelConsumer consumer) {
        return channel -> {
            // TODO: handle CompletionExceptions
            try {
                consumer.accept(channel);
            } catch (ElasticsearchStatusException e) {
                channel.sendResponse(new BytesRestResponse(channel, e.status(), e));
            } catch (ValidationException e) { // TODO: move this to where the deserialization is done
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
            } catch (Exception e) {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, e));
            }
        };
    }
}
