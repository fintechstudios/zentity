package org.elasticsearch.plugin.zentity.exceptions;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

public class NotFoundException extends ElasticsearchStatusException {
    public NotFoundException(String message) {
        this(message, null);
    }

    public NotFoundException(String message, Throwable cause) {
        super(message, RestStatus.NOT_FOUND, cause);
    }
}
