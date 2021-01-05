package io.zentity.resolution;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AttributeIdConfidenceScoreMapTest {
    static final String ATTRIBUTE_NAME = "attr";
    static final String MATCHER_NAME = "matcher";
    static final String INDEX_NAME = "idx";
    static final String INDEX_FIELD_NAME = "idxFieldName";

    @Test
    public void testGetScore() {
        AttributeIdConfidenceScoreMap map = new AttributeIdConfidenceScoreMap();
        double score = 0.5d;
        map.setScore(ATTRIBUTE_NAME, MATCHER_NAME, INDEX_NAME, INDEX_FIELD_NAME, score);
        assertEquals((Double) score, map.getScore(ATTRIBUTE_NAME, MATCHER_NAME, INDEX_NAME, INDEX_FIELD_NAME));
    }

    @Test
    public void testSetScore() {
        AttributeIdConfidenceScoreMap map = new AttributeIdConfidenceScoreMap();
        double score = 0.5d;
        assertEquals((Double) score, map.setScore(ATTRIBUTE_NAME, MATCHER_NAME, INDEX_NAME, INDEX_FIELD_NAME, score));
    }

    @Test
    public void testSetScoreOverwrite() {
        AttributeIdConfidenceScoreMap map = new AttributeIdConfidenceScoreMap();
        double score1 = 0.5d;
        assertEquals((Double) score1, map.setScore(ATTRIBUTE_NAME, MATCHER_NAME, INDEX_NAME, INDEX_FIELD_NAME, score1));
        assertEquals((Double) score1, map.getScore(ATTRIBUTE_NAME, MATCHER_NAME, INDEX_NAME, INDEX_FIELD_NAME));

        double score2 = 0.75d;
        assertEquals((Double) score2, map.setScore(ATTRIBUTE_NAME, MATCHER_NAME, INDEX_NAME, INDEX_FIELD_NAME, score2));
        assertEquals((Double) score2, map.getScore(ATTRIBUTE_NAME, MATCHER_NAME, INDEX_NAME, INDEX_FIELD_NAME));
    }

    @Test
    public void testHasScore() {
        AttributeIdConfidenceScoreMap map = new AttributeIdConfidenceScoreMap();
        double score = 0.5d;
        map.setScore(ATTRIBUTE_NAME, MATCHER_NAME, INDEX_NAME, INDEX_FIELD_NAME, score);

        assertTrue(map.hasScore(ATTRIBUTE_NAME, MATCHER_NAME, INDEX_NAME, INDEX_FIELD_NAME));
    }

    @Test
    public void testDoNotHasScore() {
        AttributeIdConfidenceScoreMap map = new AttributeIdConfidenceScoreMap();
        double score = 0.5d;
        map.setScore(ATTRIBUTE_NAME, MATCHER_NAME, INDEX_NAME, INDEX_FIELD_NAME, score);

        assertFalse(map.hasScore(ATTRIBUTE_NAME, MATCHER_NAME, "another-index", INDEX_FIELD_NAME));
    }
}
