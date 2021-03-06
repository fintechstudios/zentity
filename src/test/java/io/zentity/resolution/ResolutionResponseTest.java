package io.zentity.resolution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.zentity.common.StreamUtil;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.index.query.QueryBuilder;
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

import static io.zentity.devtools.JsonTestUtil.assertUnorderedEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class ResolutionResponseTest {

    static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    private byte[] readResourceFile(String resourcePath) throws IOException {
        InputStream stream = getClass().getResourceAsStream(resourcePath);
        return IOUtils.toByteArray(stream);
    }

    static LoggedQuery createLoggedQuery(int num) {
        LoggedSearch search = new LoggedSearch();

        SearchRequestBuilder searchRequest = new SearchRequestBuilder(
            mock(ElasticsearchClient.class),
            SearchAction.INSTANCE
        );
        QueryBuilder query = QueryBuilders.boolQuery().mustNot(QueryBuilders.matchAllQuery());
        searchRequest.setQuery(query);

        search.searchRequest = searchRequest;
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

        LoggedQuery loggedQuery = new LoggedQuery();
        loggedQuery.index = ".zentity-test-index";
        loggedQuery.hop = num + 3;
        loggedQuery.queryNumber = num;
        loggedQuery.search = search;
        loggedQuery.filters = filters;
        return loggedQuery;
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
        response.hits = StreamUtil
            .fromIterator(hitsArr.iterator())
            .collect(Collectors.toList());

        String actualJson = MAPPER.writeValueAsString(response);
        JsonNode actual = MAPPER.readTree(actualJson);

        String expectedJson = "{\"took\":120000,\"hits\":{\"total\":2,\"hits\":[{\"_index\":\".zentity-test-index\",\"_id\":101,\"_hop\":7,\"_query\":4,\"_score\":0.6,\"_attributes\":{\"city_name_attributes\":[\"name\",\"city\"]},\"_explanation\":{\"resolvers\":{\"city_name_resolver\":{\"attributes\":[\"name\",\"city\"]}},\"matches\":[{\"attribute\":\"name\",\"target_field\":\"name_target\",\"target_value\":\"Alice\",\"input_value\":\"Alice\",\"input_matcher\":\"name_exact_matcher\",\"input_matcher_params\":{},\"score\":0.5},{\"attribute\":\"city\",\"target_field\":\"city_target\",\"target_value\":\"New York\",\"input_value\":\"New York\",\"input_matcher\":\"city_exact_matcher\",\"input_matcher_params\":{},\"score\":0.7}]},\"_source\":{\"name\":\"Alice\",\"city\":\"New York\"}},{\"_index\":\".zentity-test-index\",\"_id\":102,\"_hop\":6,\"_query\":3,\"_score\":0.5,\"_attributes\":{\"city_name_attributes\":[\"name\",\"city\"]},\"_explanation\":{\"resolvers\":{\"city_name_resolver\":{\"attributes\":[\"name\",\"city\"]}},\"matches\":[{\"attribute\":\"name\",\"target_field\":\"name_target\",\"target_value\":\"Alic\",\"input_value\":\"Alice\",\"input_matcher\":\"name_exact_matcher\",\"input_matcher_params\":{},\"score\":0.3},{\"attribute\":\"city\",\"target_field\":\"city_target\",\"target_value\":\"New York\",\"input_value\":\"New York\",\"input_matcher\":\"city_exact_matcher\",\"input_matcher_params\":{},\"score\":0.7}]},\"_source\":{\"name\":\"Alic\",\"city\":\"New York\"}}]},\"queries\":[{\"_index\":\".zentity-test-index\",\"_hop\":3,\"_query\":0,\"search\":{\"request\":{\"query\":{\"bool\":{\"must_not\":[{\"match_all\":{\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}},\"response\":{\"error\":{\"reason\":\"This was not found\",\"type\":\"status_exception\",\"status\":404,\"root_cause\":[{\"type\":\"status_exception\",\"reason\":\"This was not found\"}]}}},\"filters\":{\"attributes\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}},\"terms\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}}}},{\"_index\":\".zentity-test-index\",\"_hop\":5,\"_query\":2,\"search\":{\"request\":{\"query\":{\"bool\":{\"must_not\":[{\"match_all\":{\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}},\"response\":{\"error\":{\"reason\":\"This was not found\",\"type\":\"status_exception\",\"status\":404,\"root_cause\":[{\"type\":\"status_exception\",\"reason\":\"This was not found\"}]}}},\"filters\":{\"attributes\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}},\"terms\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}}}},{\"_index\":\".zentity-test-index\",\"_hop\":7,\"_query\":4,\"search\":{\"request\":{\"query\":{\"bool\":{\"must_not\":[{\"match_all\":{\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}},\"response\":{\"error\":{\"reason\":\"This was not found\",\"type\":\"status_exception\",\"status\":404,\"root_cause\":[{\"type\":\"status_exception\",\"reason\":\"This was not found\"}]}}},\"filters\":{\"attributes\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}},\"terms\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}}}}]}";
        JsonNode expected = MAPPER.readTree(expectedJson);

        assertUnorderedEquals(expected, actual);
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
        String actualJson = MAPPER.writeValueAsString(response)
            .replaceAll("\\\\r", "")
            .replaceAll("\\\\n", "");

        JsonNode actual = MAPPER.readTree(actualJson);

        String expectedJson = "{\"took\":60000,\"hits\":{\"total\":0,\"hits\":[]},\"error\":{\"by\":\"zentity\",\"type\":\"java.lang.RuntimeException\",\"reason\":\"woops!\",\"stack_trace\":\"java.lang.RuntimeException: woops!\\tat TotalBogus.bungledMethod(NotThisFile.java:3)\"}}";
        JsonNode expected = MAPPER.readTree(expectedJson);

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

        String actualJson = MAPPER.writeValueAsString(response);
        JsonNode actual = MAPPER.readTree(actualJson);

        String expectedJson = "{\"took\":60000,\"hits\":{\"total\":0,\"hits\":[]},\"error\":{\"by\":\"elasticsearch\",\"type\":\"org.elasticsearch.ElasticsearchStatusException\",\"reason\":\"This was not found\"}}";
        JsonNode expected = MAPPER.readTree(expectedJson);

        assertEquals(expected, actual);
    }
}
