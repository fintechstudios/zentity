package io.zentity.resolution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;

public class ResolutionResponseTest {

    static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    private byte[] readResourceFile(String resourcePath) throws IOException {
        InputStream stream = getClass().getResourceAsStream(resourcePath);
        return IOUtils.toByteArray(stream);
    }

    static LoggedQuery createLoggedQuery(int num) {
        LoggedSearch search = new LoggedSearch();
        search.searchRequest = QueryBuilders.boolQuery().mustNot(QueryBuilders.matchAllQuery());
        search.responseError = new ElasticsearchStatusException("This was not found", RestStatus.NOT_FOUND);

        LoggedFilter attributesFilter = new LoggedFilter();
        Map<String, Collection<String>> attributesFilterResolverAttributes = new HashMap<>();
        attributesFilterResolverAttributes.put("name", Arrays.asList("Alice Jones", "Alice"));
        attributesFilter.resolverAttributes = attributesFilterResolverAttributes;
        Map<Integer, FilterTree> attributesFilterGroupedTree = new HashMap<>();
        FilterTree attributesFilterTree1 = new FilterTree();
        attributesFilterTree1.put("name_dob", new FilterTree());
        attributesFilterGroupedTree.put(0, attributesFilterTree1);
        attributesFilter.groupedTree = attributesFilterGroupedTree;


        LoggedFilter termsFilter = new LoggedFilter();
        Map<String, Collection<String>> termsFilterResolverAttributes = new HashMap<>();
        termsFilterResolverAttributes.put("city", Arrays.asList("Alice Jones", "Alice"));
        termsFilter.resolverAttributes = termsFilterResolverAttributes;

        Map<Integer, FilterTree> termsFilterGroupedTree = new HashMap<>();
        FilterTree termsFilterTree1 = new FilterTree();
        termsFilterTree1.put("city", new FilterTree());
        termsFilterGroupedTree.put(0, termsFilterTree1);
        termsFilter.groupedTree = termsFilterGroupedTree;

        Map<String, LoggedFilter> filters = new HashMap<>();
        filters.put("attributes", attributesFilter);
        filters.put("terms", attributesFilter);

        LoggedQuery query = new LoggedQuery();
        query.index = ".zentity-test-index";
        query.hop = num + 3;
        query.queryNumber = num;
        query.search = search;
        query.filters = filters;
        return query;
    }

    @Test
    public void testSerializeNoExceptionWithQueries() throws IOException {
        ResolutionResponse response = new ResolutionResponse();
        response.took = Duration.ofMinutes(2);

        response.includeQueries = true;
        response.queries = Arrays.asList(
            createLoggedQuery(0),
            createLoggedQuery(2),
            createLoggedQuery(4)
        );

        response.error = null;

        response.includeHits = true;
        ArrayNode hitsArr = (ArrayNode) MAPPER.readTree(readResourceFile("ResolutionResponseHits.json"));
        response.hits = StreamSupport
            .stream(hitsArr.spliterator(), false)
            .collect(Collectors.toList());

        String actual = MAPPER.writeValueAsString(response);
        String expected = "{\"took\":120000,\"hits\":{\"total\":2,\"hits\":[{\"_index\":\".zentity-test-index\",\"_id\":101,\"_hop\":7,\"_query\":4,\"_score\":0.6,\"_attributes\":{\"city_name_attributes\":[\"name\",\"city\"]},\"_explanation\":{\"resolvers\":{\"city_name_resolver\":{\"attributes\":[\"name\",\"city\"]}},\"matches\":[{\"attribute\":\"name\",\"target_field\":\"name_target\",\"target_value\":\"Alice\",\"input_value\":\"Alice\",\"input_matcher\":\"name_exact_matcher\",\"input_matcher_params\":{},\"score\":0.5},{\"attribute\":\"city\",\"target_field\":\"city_target\",\"target_value\":\"New York\",\"input_value\":\"New York\",\"input_matcher\":\"city_exact_matcher\",\"input_matcher_params\":{},\"score\":0.7}]},\"_source\":{\"name\":\"Alice\",\"city\":\"New York\"}},{\"_index\":\".zentity-test-index\",\"_id\":102,\"_hop\":6,\"_query\":3,\"_score\":0.5,\"_attributes\":{\"city_name_attributes\":[\"name\",\"city\"]},\"_explanation\":{\"resolvers\":{\"city_name_resolver\":{\"attributes\":[\"name\",\"city\"]}},\"matches\":[{\"attribute\":\"name\",\"target_field\":\"name_target\",\"target_value\":\"Alic\",\"input_value\":\"Alice\",\"input_matcher\":\"name_exact_matcher\",\"input_matcher_params\":{},\"score\":0.3},{\"attribute\":\"city\",\"target_field\":\"city_target\",\"target_value\":\"New York\",\"input_value\":\"New York\",\"input_matcher\":\"city_exact_matcher\",\"input_matcher_params\":{},\"score\":0.7}]},\"_source\":{\"name\":\"Alic\",\"city\":\"New York\"}}]},\"queries\":[[{\"search\":{\"request\":{\"bool\":{\"must_not\":[{\"match_all\":{\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"response\":{\"error\":{\"root_cause\":[{\"type\":\"status_exception\",\"reason\":\"This was not found\"}],\"type\":\"status_exception\",\"reason\":\"This was not found\",\"status\":404}}},\"filters\":{\"attributes\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}},\"terms\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}}},\"_index\":\".zentity-test-index\",\"_hop\":3,\"_query\":0},{\"search\":{\"request\":{\"bool\":{\"must_not\":[{\"match_all\":{\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"response\":{\"error\":{\"root_cause\":[{\"type\":\"status_exception\",\"reason\":\"This was not found\"}],\"type\":\"status_exception\",\"reason\":\"This was not found\",\"status\":404}}},\"filters\":{\"attributes\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}},\"terms\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}}},\"_index\":\".zentity-test-index\",\"_hop\":5,\"_query\":2},{\"search\":{\"request\":{\"bool\":{\"must_not\":[{\"match_all\":{\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"response\":{\"error\":{\"root_cause\":[{\"type\":\"status_exception\",\"reason\":\"This was not found\"}],\"type\":\"status_exception\",\"reason\":\"This was not found\",\"status\":404}}},\"filters\":{\"attributes\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}},\"terms\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}}},\"_index\":\".zentity-test-index\",\"_hop\":7,\"_query\":4}]]}";
        assertEquals(expected, actual);
    }

    @Test
    public void testSerializeWithNonEsException() throws IOException {
        ResolutionResponse response = new ResolutionResponse();
        response.took = Duration.ofMinutes(1);

        response.includeQueries = false;
        Exception ex = new RuntimeException("woops!");
        ex.setStackTrace(new StackTraceElement[]{
            new StackTraceElement("TotalBogus", "bungledMethod", "NotThisFile.java", 3)
        });
        response.error = ex;

        response.includeHits = true;

        // remove line endings for windows compatibility
        String actual = MAPPER.writeValueAsString(response);
        actual = actual
            .replaceAll("\\\\r", "")
            .replaceAll("\\\\n", "");

        String expected = "{\"took\":60000,\"hits\":{\"total\":0,\"hits\":[]},\"error\":{\"by\":\"zentity\",\"type\":\"java.lang.RuntimeException\",\"reason\":\"woops!\",\"stack_trace\":\"java.lang.RuntimeException: woops!\\tat TotalBogus.bungledMethod(NotThisFile.java:3)\"}}";

        assertEquals(expected, actual);
    }

    @Test
    public void testSerializeWithEsException() throws IOException {
        ResolutionResponse response = new ResolutionResponse();
        response.took = Duration.ofMinutes(1);

        response.includeQueries = false;

        response.includeStackTrace = false;
        response.error = new ElasticsearchStatusException("This was not found", RestStatus.NOT_FOUND);

        response.includeHits = true;

        String actual = MAPPER.writeValueAsString(response);
        String expected = "{\"took\":60000,\"hits\":{\"total\":0,\"hits\":[]},\"error\":{\"by\":\"elasticsearch\",\"type\":\"org.elasticsearch.ElasticsearchStatusException\",\"reason\":\"This was not found\"}}";
        assertEquals(expected, actual);
    }
}
