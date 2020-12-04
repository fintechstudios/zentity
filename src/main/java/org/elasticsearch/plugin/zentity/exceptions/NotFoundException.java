package org.elasticsearch.plugin.zentity.exceptions;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

public class NotFoundException extends ElasticsearchStatusException {
  public NotFoundException(String message) {
    super(message, RestStatus.NOT_FOUND);
  }
}
