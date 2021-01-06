package io.zentity.devtools;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.StreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class JsonUtils {
    static List<JsonNode> listFromIterable(Iterable<JsonNode> iterable) {
        return StreamUtils
            .fromIterable(iterable)
            .collect(Collectors.toList());
    }

    static List<JsonNode> sortedListFromIterable(Iterable<JsonNode> iterable) {
        List<JsonNode> list = listFromIterable(iterable);
        list.sort(UNORDERED_COMPARATOR);
        return list;
    }

    static Comparator<JsonNode> UNORDERED_COMPARATOR = new Comparator<JsonNode>() {
        @Override
        public int compare(JsonNode node1, JsonNode node2) {
            // will not sort "equivalent" number classes, like Doubles and Longs, in "logical" order,
            // but should not matter for true equality tests where 5.0d != 5l
            if (node1.getClass() != node2.getClass()) {
                return node1.getClass().getCanonicalName().compareTo(node2.getClass().getCanonicalName());
            }

            if (node1.isNumber()) {
                return (int) (node1.numberValue().doubleValue() - node2.numberValue().doubleValue());
            }

            if (node1.isTextual()) {
                return node1.textValue().compareTo(node2.textValue());
            }

            if (node1.size() != node2.size()) {
                return node1.size() - node2.size();
            }

            List<JsonNode> list1 = sortedListFromIterable(node1);
            List<JsonNode> list2 = sortedListFromIterable(node2);

            int sum = 0;
            for (int i = 0; i < list1.size(); i += 1) {
                sum += this.compare(list1.get(i), list2.get(i));
            }

            return sum;
        }
    };

    /**
     * Order-independent equality check.
     *
     * @param node1 A JsonNode.
     * @param node2 Another JsonNode.
     * @return If the nodes are equal, once sorted.
     */
    public static boolean unorderedEquals(JsonNode node1, JsonNode node2) {
        if (node1.size() != node2.size()) {
            return false;
        }

        if (node1.size() == 0) {
            return node1.equals(node2);
        }

        List<JsonNode> list1 = sortedListFromIterable(node1);
        List<JsonNode> list2 = sortedListFromIterable(node2);

        for (int i = 0; i < list1.size(); i += 1) {
            if (!unorderedEquals(list1.get(i), list2.get(i))) {
                return false;
            }
        }

        return true;
    }
}
