package io.zentity.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.zentity.resolution.FilterTree;
import org.junit.Test;

import java.util.Collections;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class JsonTest {
    @Test
    public void testSerializeFilterTree() throws JsonProcessingException {
        FilterTree nameTree = new FilterTree();
        nameTree.put("street", new FilterTree());
        FilterTree root = new FilterTree();
        root.put("name", nameTree);
        String serialized = Json.ORDERED_MAPPER.writeValueAsString(root);
        assertEquals("{\"name\":{\"street\":{}}}", serialized);
    }

    @Test
    public void testSerializeNestedFilterTree() throws JsonProcessingException {
        TreeMap<Integer, FilterTree> nestedTree = new TreeMap<>(Collections.reverseOrder());
        nestedTree.put(0, new FilterTree());
        String serialized = Json.ORDERED_MAPPER.writeValueAsString(nestedTree);
        assertEquals("{\"0\":{}}", serialized);
    }
}
