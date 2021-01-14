package org.elasticsearch.plugin.zentity;

import org.elasticsearch.rest.BaseRestHandler;

public abstract class BaseZentityAction extends BaseRestHandler {
    protected final ZentityConfig config;

    public BaseZentityAction(ZentityConfig config) {
        this.config = config;
    }
}
