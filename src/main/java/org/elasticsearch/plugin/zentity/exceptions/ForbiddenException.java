package org.elasticsearch.plugin.zentity.exceptions;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestStatus;

public class ForbiddenException extends ElasticsearchSecurityException {
  public ForbiddenException(String message) {
    super(message, RestStatus.FORBIDDEN);
  }
}
