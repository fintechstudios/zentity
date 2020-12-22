package io.zentity.resolution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LoggedQueryTest {
    static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    @Test
    public void testSerialize() throws JsonProcessingException {
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
        query.hop = 3;
        query.queryNumber = 4;
        query.search = search;
        query.filters = filters;

        String json = MAPPER.writeValueAsString(query);
        assertEquals(json, "{\"search\":{\"request\":{\n" +
            "  \"bool\" : {\n" +
            "    \"must_not\" : [\n" +
            "      {\n" +
            "        \"match_all\" : {\n" +
            "          \"boost\" : 1.0\n" +
            "        }\n" +
            "      }\n" +
            "    ],\n" +
            "    \"adjust_pure_negative\" : true,\n" +
            "    \"boost\" : 1.0\n" +
            "  }\n" +
            "},\"response\":{\"error\":{\"root_cause\":[{\"type\":\"status_exception\",\"reason\":\"This was not found\"}],\"type\":\"status_exception\",\"reason\":\"This was not found\",\"status\":404}}},\"filters\":{\"attributes\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}},\"terms\":{\"resolvers\":{\"name\":{\"attributes\":[\"Alice Jones\",\"Alice\"]}},\"tree\":{\"0\":{\"name_dob\":{}}}}},\"_index\":\".zentity-test-index\",\"_hop\":3,\"_query\":4}");
    }
}
