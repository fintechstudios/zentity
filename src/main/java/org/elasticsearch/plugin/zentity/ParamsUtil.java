package org.elasticsearch.plugin.zentity;

import org.elasticsearch.rest.RestRequest;

import java.util.Optional;

public class ParamsUtil {
    public static Optional<String> opt(RestRequest req, String key) {
        return Optional.ofNullable(req.param(key));
    }

    public static Boolean optBoolean(RestRequest req, String key, Boolean defaultVal) {
        return opt(req, key)
            .map(Boolean::valueOf)
            .orElse(defaultVal);
    }

    public static Boolean optBoolean(RestRequest req, String key) {
        return optBoolean(req, key, null);
    }

    public static Integer optInteger(RestRequest req, String key, Integer defaultVal) {
        return opt(req, key)
            .map(Integer::parseInt)
            .orElse(defaultVal);
    }

    public static Integer optInteger(RestRequest req, String key) {
        return optInteger(req, key, null);
    }
}
