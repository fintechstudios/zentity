package io.zentity.resolution;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class LoggedQuery {
    // _index
    @JsonProperty("_index")
    public String index;
    // _hop
    @JsonProperty("_hop")
    public int hop;
    // _query
    @JsonProperty("_query")
    public int queryNumber;
    // search
    public LoggedSearch search;
    // filters
    public Map<String, LoggedFilter> filters;
}
