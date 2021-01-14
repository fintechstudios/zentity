package io.zentity.resolution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static io.zentity.devtools.JsonTestUtil.assertUnorderedEquals;
import static org.mockito.Mockito.mock;

public class LoggedSearchTest {
    static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    @Test
    public void testSerializeException() throws JsonProcessingException {
        LoggedSearch search = new LoggedSearch();

        SearchRequestBuilder searchRequest = new SearchRequestBuilder(
            mock(ElasticsearchClient.class),
            SearchAction.INSTANCE
        );
        QueryBuilder query = QueryBuilders.matchAllQuery();
        searchRequest.setQuery(query);

        search.searchRequest = searchRequest;
        search.responseError = new ElasticsearchStatusException("This was not found", RestStatus.NOT_FOUND);

        String actualJson = MAPPER.writeValueAsString(search);
        JsonNode actual = MAPPER.readTree(actualJson);

        String expectedJson = "{\"request\":{\"query\":{\"match_all\":{\"boost\":1.0}}},\"response\":{\"error\":{\"root_cause\":[{\"type\":\"status_exception\",\"reason\":\"This was not found\"}],\"type\":\"status_exception\",\"reason\":\"This was not found\",\"status\":404}}}";
        JsonNode expected = MAPPER.readTree(expectedJson);
        assertUnorderedEquals(expected, actual);
    }

    @Test
    public void testSerializeParseException() throws JsonProcessingException {
        LoggedSearch search = new LoggedSearch();

        SearchRequestBuilder searchRequest = new SearchRequestBuilder(
            mock(ElasticsearchClient.class),
            SearchAction.INSTANCE
        );
        QueryBuilder query = QueryBuilders.matchAllQuery();
        searchRequest.setQuery(query);

        search.searchRequest = searchRequest;
        search.responseError = new XContentParseException(new XContentLocation(1, 2), "Something ain't parsin'!");

        String actualJson = MAPPER.writeValueAsString(search);
        JsonNode actual = MAPPER.readTree(actualJson);

        String expectedJson = "{\"request\":{\"query\":{\"match_all\":{\"boost\":1.0}}},\"response\":{\"error\":{\"reason\":\"[1:2] Something ain't parsin'!\",\"type\":\"parsing_exception\",\"line\":1,\"col\":2,\"status\":400,\"root_cause\":[{\"type\":\"parsing_exception\",\"line\":1,\"col\":2,\"status\":400,\"reason\":\"[1:2] Something ain't parsin'!\"}]}}}";
        JsonNode expected = MAPPER.readTree(expectedJson);
        assertUnorderedEquals(expected, actual);
    }

    @Test
    public void testSerializeUnknownException() throws JsonProcessingException {
        LoggedSearch search = new LoggedSearch();

        SearchRequestBuilder searchRequest = new SearchRequestBuilder(
            mock(ElasticsearchClient.class),
            SearchAction.INSTANCE
        );
        QueryBuilder query = QueryBuilders.matchAllQuery();
        searchRequest.setQuery(query);

        search.searchRequest = searchRequest;
        search.responseError = new IllegalAccessError("U Can't Touch This");

        String actualJson = MAPPER.writeValueAsString(search);
        JsonNode actual = MAPPER.readTree(actualJson);

        String expectedJson = "{\"request\":{\"query\":{\"match_all\":{\"boost\":1.0}}},\"response\":{\"error\":{\"reason\":\"U Can't Touch This\",\"type\":\"java.lang.IllegalAccessError\",\"status\":500,\"root_cause\":[{\"type\":\"java.lang.IllegalAccessError\",\"status\":500,\"reason\":\"U Can't Touch This\"}]}}}";
        JsonNode expected = MAPPER.readTree(expectedJson);
        assertUnorderedEquals(expected, actual);
    }

    @Test
    public void testSerializeWithResponse() throws JsonProcessingException {
        LoggedSearch search = new LoggedSearch();

        SearchRequestBuilder searchRequest = new SearchRequestBuilder(
            mock(ElasticsearchClient.class),
            SearchAction.INSTANCE
        );
        QueryBuilder query = QueryBuilders.matchAllQuery();
        searchRequest.setQuery(query);

        search.searchRequest = searchRequest;
        SearchResponseSections sections = new SearchResponseSections(
            SearchHits.empty(true),
            new Aggregations(Collections.emptyList()),
            new Suggest(Collections.emptyList()),
            false,
            null,
            new SearchProfileShardResults(new HashMap<>()),
            1
        );
        search.response = new SearchResponse(
            sections,
            "some-scroll-id",
            3,
            2,
            1,
            300,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        String actualJson = MAPPER.writeValueAsString(search);
        JsonNode actual = MAPPER.readTree(actualJson);

        String expectedJson = "{\"request\":{\"query\":{\"match_all\":{\"boost\":1.0}}},\"response\":{\"_scroll_id\":\"some-scroll-id\",\"took\":300,\"timed_out\":false,\"_shards\":{\"total\":3,\"successful\":2,\"skipped\":1,\"failed\":0},\"hits\":{\"total\":{\"value\":0,\"relation\":\"eq\"},\"max_score\":0.0,\"hits\":[]},\"suggest\":{},\"profile\":{\"shards\":[]}}}";
        JsonNode expected = MAPPER.readTree(expectedJson);

        assertUnorderedEquals(expected, actual);
    }
}
