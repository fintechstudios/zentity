package io.zentity.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.NumericNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static final ObjectMapper ORDERED_MAPPER = new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    /**
     * Converts an object {@link JsonNode JsonNode's} fields iterator to a {@link Map} of strings.
     *
     * @param iterator The object iterator.
     * @return The node's map representation.
     * @throws JsonProcessingException If the object cannot be written as a string.
     */
    public static Map<String, String> toStringMap(Iterator<Map.Entry<String, JsonNode>> iterator) throws JsonProcessingException {
        Map<String, String> map = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> paramNode = iterator.next();
            String paramField = paramNode.getKey();
            JsonNode paramValue = paramNode.getValue();
            if (paramValue.isObject() || paramValue.isArray()) {
                map.put(paramField, MAPPER.writeValueAsString(paramValue));
            } else if (paramValue.isNull()) {
                map.put(paramField, "null");
            } else {
                map.put(paramField, paramValue.asText());
            }
        }
        return map;
    }

    /**
     * Converts an object {@link JsonNode} to a {@link Map} of strings.
     *
     * @param node The object node.
     * @return The node's map representation.
     * @throws JsonProcessingException If the object cannot be written as a string.
     */
    public static Map<String, String> toStringMap(JsonNode node) throws JsonProcessingException {
        if (!node.isObject()) {
            throw new IllegalArgumentException("Can only convert JSON objects to maps");
        }
        return toStringMap(node.fields());
    }

    /**
     * Converts an object JSON {@link String} to a {@link Map} of strings.
     *
     * @param jsonString The object node string.
     * @return The node's map representation.
     * @throws JsonProcessingException If the object cannot be written/ parsed as a string.
     */
    public static Map<String, String> toStringMap(String jsonString) throws JsonProcessingException {
       return toStringMap(Json.MAPPER.readTree(jsonString));
    }

    public static NumericNode parseNumberAsNode(String num) throws IOException {
        return Json.MAPPER.readValue(num, NumericNode.class);
    }
}
