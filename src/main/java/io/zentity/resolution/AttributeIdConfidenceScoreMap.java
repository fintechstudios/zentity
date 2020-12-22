package io.zentity.resolution;

import java.util.HashMap;

/**
 * A map to cache confidence scores of attribute identities.
 */
public class AttributeIdConfidenceScoreMap extends HashMap<String, Double> {
    private static String makeKey(String attributeName, String matcherName, String indexName, String indexFieldName) {
        return attributeName + "/" + matcherName + "/" + indexName + "/" + indexFieldName;
    }

    boolean hasScore(String attributeName, String matcherName, String indexName, String indexFieldName) {
        String key = makeKey(attributeName, matcherName, indexName, indexFieldName);
        return this.containsKey(key);
    }

    Double getScore(String attributeName, String matcherName, String indexName, String indexFieldName) {
        String key = makeKey(attributeName, matcherName, indexName, indexFieldName);
        return this.get(key);
    }

    Double setScore(String attributeName, String matcherName, String indexName, String indexFieldName, double score) {
        String key = makeKey(attributeName, matcherName, indexName, indexFieldName);
        return this.put(key, score);
    }
}
