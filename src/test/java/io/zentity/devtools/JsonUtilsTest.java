package io.zentity.devtools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import org.junit.Test;

import static io.zentity.devtools.JsonUtils.unorderedEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonUtilsTest {

    @Test
    public void testJsonUnorderedEqualsEqual() throws JsonProcessingException {
        JsonNode node1 = Json.ORDERED_MAPPER.readTree("{\"string\":[\"\",\"abc\"],\"array\":[\"111\",\"222\",\"333\",\"444\"]}");
        JsonNode node2 = Json.ORDERED_MAPPER.readTree("{\"string\":[\"abc\",\"\"],\"array\":[\"222\",\"444\",\"333\",\"111\"]}");
        assertTrue(unorderedEquals(node1, node2));
    }

    @Test
    public void testJsonUnorderedEqualsNotEqual() throws JsonProcessingException {
        JsonNode node1 = Json.ORDERED_MAPPER.readTree("{\"string\":[\"\",\"abc\"],\"array\":[\"11\",\"222\",\"333\",\"444\"]}");
        JsonNode node2 = Json.ORDERED_MAPPER.readTree("{\"string\":[\"abc\",\"\"],\"array\":[\"222\",\"333\",\"444\",\"111\"]}");
        assertFalse(unorderedEquals(node1, node2));
    }
}
