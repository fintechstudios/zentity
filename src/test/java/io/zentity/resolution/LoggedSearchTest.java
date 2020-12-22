package io.zentity.resolution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
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

public class LoggedSearchTest {
    static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    @Test
    public void testSerializeException() throws JsonProcessingException {
        LoggedSearch search = new LoggedSearch();

        search.searchRequest = QueryBuilders.matchAllQuery();
        search.responseError = new ElasticsearchStatusException("This was not found", RestStatus.NOT_FOUND);

        String actual = MAPPER.writeValueAsString(search);
        String expected = "{\"request\":{\"match_all\":{\"boost\":1.0}},\"response\":{\"error\":{\"root_cause\":[{\"type\":\"status_exception\",\"reason\":\"This was not found\"}],\"type\":\"status_exception\",\"reason\":\"This was not found\",\"status\":404}}}";
        assertEquals(expected, actual);
    }

    @Test
    public void testSerializeWithResponse() throws JsonProcessingException {
        LoggedSearch search = new LoggedSearch();

        search.searchRequest = QueryBuilders.matchAllQuery();
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
            new ShardSearchFailure[]{},
            SearchResponse.Clusters.EMPTY
        );

        String actual = MAPPER.writeValueAsString(search);
        String expected = "{\"request\":{\"match_all\":{\"boost\":1.0}},\"response\":{\"_scroll_id\":\"some-scroll-id\",\"took\":300,\"timed_out\":false,\"_shards\":{\"total\":3,\"successful\":2,\"skipped\":1,\"failed\":0},\"hits\":{\"total\":{\"value\":0,\"relation\":\"eq\"},\"max_score\":0.0,\"hits\":[]},\"suggest\":{},\"profile\":{\"shards\":[]}}}";
        assertEquals(expected, actual);
    }
}
