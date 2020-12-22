package io.zentity.resolution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LoggedFilterTest {
    static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    @Test
    public void testSerialize() throws JsonProcessingException {
        Map<String, Collection<String>> attributes = new HashMap<>();
        attributes.put("name", Arrays.asList("Alice Jones", "Alice"));

        Map<Integer, FilterTree> groupedTree = new HashMap<>();
        FilterTree filterTree1 = new FilterTree();
        filterTree1.put("name_dob", new FilterTree());
        groupedTree.put(0, filterTree1);

        LoggedFilter loggedFilter = new LoggedFilter();
        loggedFilter.resolverAttributes = attributes;
        loggedFilter.groupedTree = groupedTree;

        String json = MAPPER.writeValueAsString(loggedFilter);
        assertEquals(json, "{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}}");
    }
}
