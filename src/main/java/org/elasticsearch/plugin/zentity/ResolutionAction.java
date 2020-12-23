package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.ObjectWriter;
import io.zentity.common.Json;
import io.zentity.model.Model;
import io.zentity.resolution.Job;
import io.zentity.resolution.Job.JobResult;
import io.zentity.resolution.input.Input;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugin.zentity.exceptions.BadRequestException;
import org.elasticsearch.plugin.zentity.exceptions.NotFoundException;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ResolutionAction extends BaseAction {

    @Inject
    ResolutionAction(RestController controller) {
        controller.registerHandler(POST, "_zentity/resolution", this);
        controller.registerHandler(POST, "_zentity/resolution/{entity_type}", this);
    }

    @Override
    public String getName() {
        return "zentity_resolution_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        String body = restRequest.content().utf8ToString();

        // Parse the request params that will be passed to the job configuration
        final String entityType = restRequest.param("entity_type");
        final boolean includeAttributes = restRequest.paramAsBoolean("_attributes", Job.DEFAULT_INCLUDE_ATTRIBUTES);
        final boolean includeErrorTrace = restRequest.paramAsBoolean("error_trace", Job.DEFAULT_INCLUDE_ERROR_TRACE);
        final boolean includeExplanation = restRequest.paramAsBoolean("_explanation", Job.DEFAULT_INCLUDE_EXPLANATION);
        final boolean includeHits = restRequest.paramAsBoolean("hits", Job.DEFAULT_INCLUDE_HITS);
        final boolean includeQueries = restRequest.paramAsBoolean("queries", Job.DEFAULT_INCLUDE_QUERIES);
        final boolean includeScore = restRequest.paramAsBoolean("_score", Job.DEFAULT_INCLUDE_SCORE);
        final boolean includeSeqNoPrimaryTerm = restRequest.paramAsBoolean("_seq_no_primary_term", Job.DEFAULT_INCLUDE_SEQ_NO_PRIMARY_TERM);
        final boolean includeSource = restRequest.paramAsBoolean("_source", Job.DEFAULT_INCLUDE_SOURCE);
        final boolean includeVersion = restRequest.paramAsBoolean("_version", Job.DEFAULT_INCLUDE_VERSION);
        final int maxDocsPerQuery = restRequest.paramAsInt("max_docs_per_query", Job.DEFAULT_MAX_DOCS_PER_QUERY);
        final int maxHops = restRequest.paramAsInt("max_hops", Job.DEFAULT_MAX_HOPS);
        final String maxTimePerQuery = restRequest.param("max_time_per_query", Job.DEFAULT_MAX_TIME_PER_QUERY);
        final boolean pretty = restRequest.paramAsBoolean("pretty", false);
        final boolean profile = restRequest.paramAsBoolean("profile", Job.DEFAULT_PROFILE);

        // Parse any optional search parameters that will be passed to the job configuration.
        // Note: org.elasticsearch.rest.RestRequest doesn't allow null values as default values for integer parameters,
        // which is why the code below handles the integer parameters differently from the others.
        final Boolean searchAllowPartialSearchResults = ParamsUtil.optBoolean(restRequest, "search.allow_partial_search_results");
        final Integer searchBatchedReduceSize = ParamsUtil.optInteger(restRequest, "search.batched_reduce_size");
        final Integer searchMaxConcurrentShardRequests = ParamsUtil.optInteger(restRequest, "search.max_concurrent_shard_requests");
        final Integer searchPreFilterShardSize = ParamsUtil.optInteger(restRequest, "search.pre_filter_shard_size");
        final Boolean searchRequestCache = ParamsUtil.optBoolean(restRequest, "search.request_cache");
        final String searchPreference = restRequest.param("search.preference");

        return errorHandlingConsumer(channel -> {
            // Validate the request body.
            if (body == null || body.equals("")) {
                throw new BadRequestException("Request body is missing.");
            }

            // Parse and validate the job input.
            Input input;
            if (entityType == null || entityType.equals("")) {
                input = new Input(body);
            } else {
                GetResponse getResponse = ModelsAction.getEntityModel(entityType, client);
                if (!getResponse.isExists())
                    throw new NotFoundException("Entity type '" + entityType + "' not found.");
                String model = getResponse.getSourceAsString();
                input = new Input(body, new Model(model));
            }

            // Prepare the entity resolution job.
            Job job = Job.newBuilder()
                .client(client)
                .includeAttributes(includeAttributes)
                .includeErrorTrace(includeErrorTrace)
                .includeExplanation(includeExplanation)
                .includeHits(includeHits)
                .includeQueries(includeQueries)
                .includeScore(includeScore)
                .includeSeqNoPrimaryTerm(includeSeqNoPrimaryTerm)
                .includeSource(includeSource)
                .includeVersion(includeVersion)
                .maxDocsPerQuery(maxDocsPerQuery)
                .maxHops(maxHops)
                .maxTimePerQuery(maxTimePerQuery)
                .profile(profile)
                .input(input)
                .searchAllowPartialSearchResults(searchAllowPartialSearchResults)
                .searchBatchedReduceSize(searchBatchedReduceSize)
                .searchMaxConcurrentShardRequests(searchMaxConcurrentShardRequests)
                .searchPreFilterShardSize(searchPreFilterShardSize)
                .searchPreference(searchPreference)
                .searchRequestCache(searchRequestCache)
                .build();

            // Run the entity resolution job.
            JobResult result = job.run();
            ObjectWriter writer = pretty
                ? Json.ORDERED_MAPPER.writerWithDefaultPrettyPrinter()
                : Json.MAPPER.writer();
            String responseJson = writer.writeValueAsString(result.getResponse());
            if (result.failed()) {
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "application/json", responseJson));
            } else {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", responseJson));
            }
        });
    }
}
