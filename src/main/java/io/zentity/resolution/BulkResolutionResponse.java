package io.zentity.resolution;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class BulkResolutionResponse {
    // took, in ms
    @JsonProperty("took")
    public long tookMs;

    // whether some of the responses are failures
    public boolean errors;

    // all the responses in the request
    public List<ResolutionResponse> items;
}
