package org.elasticsearch.plugin.zentity.exceptions;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

public class NotImplementedException extends ElasticsearchStatusException {
  public NotImplementedException(String message) {
    super(message, RestStatus.NOT_IMPLEMENTED);
  }
}
