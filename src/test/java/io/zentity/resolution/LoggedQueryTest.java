package io.zentity.resolution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class LoggedQueryTest {
    static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    @Test
    public void testSerialize() throws JsonProcessingException {
        LoggedSearch loggedSearch = new LoggedSearch();

        SearchRequestBuilder searchRequest = new SearchRequestBuilder(
            mock(ElasticsearchClient.class),
            SearchAction.INSTANCE
        );
        QueryBuilder query = QueryBuilders.boolQuery().mustNot(QueryBuilders.matchAllQuery());
        searchRequest.setQuery(query);
        searchRequest.setFetchSource(true);

        loggedSearch.searchRequest = searchRequest;
        loggedSearch.responseError = new ElasticsearchStatusException("This was not found", RestStatus.NOT_FOUND);

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
        loggedQuery.hop = 3;
        loggedQuery.queryNumber = 4;
        loggedQuery.search = loggedSearch;
        loggedQuery.filters = filters;

        String actual = MAPPER.writeValueAsString(loggedQuery);
        String expected = "{\"search\":{\"request\":{\"query\":{\"bool\":{\"must_not\":[{\"match_all\":{\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"_source\":{\"includes\":[],\"excludes\":[]}},\"response\":{\"error\":{\"root_cause\":[{\"type\":\"status_exception\",\"reason\":\"This was not found\"}],\"type\":\"status_exception\",\"reason\":\"This was not found\",\"status\":404}}},\"filters\":{\"attributes\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}},\"terms\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}}},\"_index\":\".zentity-test-index\",\"_hop\":3,\"_query\":4}";
        assertEquals(expected, actual);
    }
}
