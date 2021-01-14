package org.elasticsearch.plugin.zentity.exceptions;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestStatus;

public class ForbiddenException extends ElasticsearchSecurityException {
    public ForbiddenException(String message) {
        this(message, null);
    }

    public ForbiddenException(String message, Throwable cause) {
        super(message, RestStatus.FORBIDDEN, cause);
    }
}
