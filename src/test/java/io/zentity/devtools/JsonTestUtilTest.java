package io.zentity.devtools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import org.junit.ComparisonFailure;
import org.junit.Test;

import static io.zentity.devtools.JsonTestUtil.assertUnorderedEquals;
import static io.zentity.devtools.JsonTestUtil.unorderedEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonTestUtilTest {

    @Test
    public void testUnorderedEqualsNullFirst() throws JsonProcessingException {
        JsonNode node2 = Json.ORDERED_MAPPER.readTree("{\"string\":[\"abc\",\"\"],\"array\":[\"222\",\"444\",\"333\",\"111\"]}");
        assertFalse(unorderedEquals(null, node2));
    }

    @Test
    public void testUnorderedEqualsNullSecond() throws JsonProcessingException {
        JsonNode node1 = Json.ORDERED_MAPPER.readTree("{\"string\":[\"abc\",\"\"],\"array\":[\"222\",\"444\",\"333\",\"111\"]}");
        assertFalse(unorderedEquals(node1, null));
    }

    @Test
    public void testUnorderedEqualsBothNull() {
        assertTrue(unorderedEquals(null, null));
    }

    @Test
    public void testUnorderedEqualsEqual() throws JsonProcessingException {
        JsonNode node1 = Json.ORDERED_MAPPER.readTree("{\"string\":[\"\",\"abc\"],\"array\":[\"111\",\"222\",\"333\",\"444\"]}");
        JsonNode node2 = Json.ORDERED_MAPPER.readTree("{\"string\":[\"abc\",\"\"],\"array\":[\"222\",\"444\",\"333\",\"111\"]}");
        assertTrue(unorderedEquals(node1, node2));
    }

    @Test
    public void testUnorderedEqualsNotEqual() throws JsonProcessingException {
        JsonNode node1 = Json.ORDERED_MAPPER.readTree("{\"string\":[\"\",\"abc\"],\"array\":[\"11\",\"222\",\"333\",\"444\"]}");
        JsonNode node2 = Json.ORDERED_MAPPER.readTree("{\"string\":[\"abc\",\"\"],\"array\":[\"222\",\"333\",\"444\",\"111\"]}");
        assertFalse(unorderedEquals(node1, node2));
    }

    @Test
    public void testAssertUnorderedEqualsEqual() throws JsonProcessingException {
        JsonNode expected = Json.ORDERED_MAPPER.readTree("{\"string\":[\"\",\"abc\"],\"array\":[\"111\",\"222\",\"333\",\"444\"]}");
        JsonNode actual = Json.ORDERED_MAPPER.readTree("{\"string\":[\"abc\",\"\"],\"array\":[\"222\",\"444\",\"333\",\"111\"]}");
        assertUnorderedEquals(expected, actual);
    }

    @Test
    public void testAssertUnorderedEqualsNotEqual() throws JsonProcessingException {
        String expectedStr = "{\"string\":[\"\",\"abc\"],\"array\":[\"11\",\"222\",\"333\",\"444\"]}";
        JsonNode expected = Json.ORDERED_MAPPER.readTree(expectedStr);
        String actualStr = "{\"string\":[\"abc\",\"\"],\"array\":[\"222\",\"333\",\"444\",\"111\"]}";
        JsonNode actual = Json.ORDERED_MAPPER.readTree(actualStr);
        // assertThrows w/ junit5
        try {
            assertUnorderedEquals(expected, actual);
            fail("expected assertion error");
        } catch (ComparisonFailure failure) {
            assertEquals("expected", failure.getExpected(), expectedStr);
            assertEquals("actual", failure.getActual(), actualStr);
        }
    }
}
