package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.zentity.common.CompletableFutureUtil;
import io.zentity.common.FunctionalUtil.UnCheckedFunction;
import io.zentity.common.FunctionalUtil.UnCheckedSupplier;
import io.zentity.common.Json;
import io.zentity.common.SecurityUtil;
import io.zentity.common.StreamUtil;
import io.zentity.model.Model;
import io.zentity.resolution.BulkResolutionResponse;
import io.zentity.resolution.Job;
import io.zentity.resolution.ResolutionResponse;
import io.zentity.resolution.input.Input;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugin.zentity.exceptions.BadRequestException;
import org.elasticsearch.plugin.zentity.exceptions.NotFoundException;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.plugin.zentity.ActionUtil.channelErrorHandler;
import static org.elasticsearch.plugin.zentity.ActionUtil.errorHandlingConsumer;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class ResolutionAction extends BaseZentityAction {
    private final Executor resolutionExecutor;
    private final ModelsAction modelsAction;

    // All parameters known to the request
    private static final String PARAM_ENTITY_TYPE = "entity_type";
    private static final String PARAM_PRETTY = "pretty";
    private static final String PARAM_INCLUDE_ATTRIBUTES = "_attributes";
    private static final String PARAM_INCLUDE_ERROR_TRACE = "error_trace";
    private static final String PARAM_INCLUDE_EXPLANATION = "_explanation";
    private static final String PARAM_INCLUDE_HITS = "hits";
    private static final String PARAM_INCLUDE_QUERIES = "queries";
    private static final String PARAM_INCLUDE_SCORE = "_score";
    private static final String PARAM_INCLUDE_SEQ_NO_PRIMARY_TERM = "_seq_no_primary_term";
    private static final String PARAM_INCLUDE_SOURCE = "_source";
    private static final String PARAM_INCLUDE_VERSION = "_version";
    private static final String PARAM_MAX_DOCS_PER_QUERY = "max_docs_per_query";
    private static final String PARAM_MAX_HOPS = "max_hops";
    private static final String PARAM_MAX_TIME_PER_QUERY = "max_time_per_query";
    private static final String PARAM_PROFILE = "profile";
    private static final String PARAM_SEARCH_ALLOW_PARTIAL_SEARCH_RESULTS = "search.allow_partial_search_results";
    private static final String PARAM_SEARCH_BATCHED_REDUCE_SIZE = "search.batched_reduce_size";
    private static final String PARAM_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS = "search.max_concurrent_shard_requests";
    private static final String PARAM_SEARCH_PRE_FILTER_SHARD_SIZE = "search.pre_filter_shard_size";
    private static final String PARAM_SEARCH_REQUEST_CACHE = "search.request_cache";
    private static final String PARAM_SEARCH_PREFERENCE = "search.preference";

    public ResolutionAction(ZentityConfig config) {
        super(config);
        modelsAction = new ModelsAction(config);
        // setup a scaling executor that always keeps a few threads on hand but can
        // increase as the load increases
        resolutionExecutor = EsExecutors.newScaling(
            "zentity-resolution",
            3,
            this.config.getResolutionMaxConcurrentJobs(),
            60,
            TimeUnit.SECONDS,
            EsExecutors.daemonThreadFactory("zentity-resolution"),
            new ThreadContext(Settings.EMPTY)
        );
    }

    CompletableFuture<Input> getInputAsync(NodeClient client, String entityType, String body) {
        return CompletableFuture
            .supplyAsync(
                UnCheckedSupplier.from(() -> {
                    // Validate the request body.
                    if (body == null || body.equals("")) {
                        throw new BadRequestException("Request body is missing.");
                    }

                    // Parse and validate the job input.
                    if (entityType == null || entityType.equals("")) {
                        return new Input(body);
                    }
                    return null;
                }),
                resolutionExecutor
            ).thenCompose((input) -> {
                if (input != null) {
                    return CompletableFuture.completedFuture(input);
                }
                return modelsAction.getEntityModel(entityType, client)
                    .thenApply(UnCheckedFunction.from(
                        // cast needed to appease the compiler for the thrown checked exceptions
                        (CheckedFunction<GetResponse, Input, IOException>)
                            (res) -> {
                                if (!res.isExists()) {
                                    throw new NotFoundException("Entity type '" + entityType + "' not found.");
                                }
                                // TODO: build directly from response
                                String model = res.getSourceAsString();
                                return new Input(body, new Model(model));
                            }));
            })
            .exceptionally((ex) -> {
                Throwable cause = CompletableFutureUtil.getCause(ex);
                if (cause instanceof JsonParseException) {
                    throw new BadRequestException("Invalid JSON body", cause);
                }
                throw new CompletionException(cause);
            });
    }

    CompletableFuture<Job> buildJobAsync(NodeClient client, String body, Map<String, String> params, Map<String, String> reqParams) {
        final String entityType = ParamsUtil.optString(PARAM_ENTITY_TYPE, null, params, reqParams);
        return getInputAsync(client, entityType, body)
            .thenApply(
                (input) -> {
                    // Parse the request params that will be passed to the job configuration
                    final boolean includeAttributes = ParamsUtil.optBoolean(PARAM_INCLUDE_ATTRIBUTES, Job.DEFAULT_INCLUDE_ATTRIBUTES, params, reqParams);
                    final boolean includeErrorTrace = ParamsUtil.optBoolean(PARAM_INCLUDE_ERROR_TRACE, Job.DEFAULT_INCLUDE_ERROR_TRACE, params, reqParams);
                    final boolean includeExplanation = ParamsUtil.optBoolean(PARAM_INCLUDE_EXPLANATION, Job.DEFAULT_INCLUDE_EXPLANATION, params, reqParams);
                    final boolean includeHits = ParamsUtil.optBoolean(PARAM_INCLUDE_HITS, Job.DEFAULT_INCLUDE_HITS, params, reqParams);
                    final boolean includeQueries = ParamsUtil.optBoolean(PARAM_INCLUDE_QUERIES, Job.DEFAULT_INCLUDE_QUERIES, params, reqParams);
                    final boolean includeScore = ParamsUtil.optBoolean(PARAM_INCLUDE_SCORE, Job.DEFAULT_INCLUDE_SCORE, params, reqParams);
                    final boolean includeSeqNoPrimaryTerm = ParamsUtil.optBoolean(PARAM_INCLUDE_SEQ_NO_PRIMARY_TERM, Job.DEFAULT_INCLUDE_SEQ_NO_PRIMARY_TERM, params, reqParams);
                    final boolean includeSource = ParamsUtil.optBoolean(PARAM_INCLUDE_SOURCE, Job.DEFAULT_INCLUDE_SOURCE, params, reqParams);
                    final boolean includeVersion = ParamsUtil.optBoolean(PARAM_INCLUDE_VERSION, Job.DEFAULT_INCLUDE_VERSION, params, reqParams);
                    final int maxDocsPerQuery = ParamsUtil.optInteger(PARAM_MAX_DOCS_PER_QUERY, Job.DEFAULT_MAX_DOCS_PER_QUERY, params, reqParams);
                    final int maxHops = ParamsUtil.optInteger(PARAM_MAX_HOPS, Job.DEFAULT_MAX_HOPS, params, reqParams);
                    final TimeValue maxTimePerQuery = ParamsUtil.optTimeValue(PARAM_MAX_TIME_PER_QUERY, Job.DEFAULT_MAX_TIME_PER_QUERY, params, reqParams);
                    final boolean profile = ParamsUtil.optBoolean(PARAM_PROFILE, Job.DEFAULT_PROFILE, params, reqParams);

                    // Parse any optional search parameters that will be passed to the job configuration.
                    final Boolean searchAllowPartialSearchResults = ParamsUtil.optBoolean(PARAM_SEARCH_ALLOW_PARTIAL_SEARCH_RESULTS, null, params, reqParams);
                    final Integer searchBatchedReduceSize = ParamsUtil.optInteger(PARAM_SEARCH_BATCHED_REDUCE_SIZE, null, params, reqParams);
                    final Integer searchMaxConcurrentShardRequests = ParamsUtil.optInteger(PARAM_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS, null, params, reqParams);
                    final Integer searchPreFilterShardSize = ParamsUtil.optInteger(PARAM_SEARCH_PRE_FILTER_SHARD_SIZE, null, params, reqParams);
                    final Boolean searchRequestCache = ParamsUtil.optBoolean(PARAM_SEARCH_REQUEST_CACHE, null, params, reqParams);
                    final String searchPreference = ParamsUtil.optString(PARAM_SEARCH_PREFERENCE, null, params, reqParams);

                    return Job.newBuilder()
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
                }
            );
    }

    CompletableFuture<ResolutionResponse> buildAndRunJobAsync(NodeClient client, String body, Map<String, String> params, Map<String, String> reqParams) {
        return buildJobAsync(client, body, params, reqParams)
            .handleAsync((job, err) -> {
                if (err == null) {
                    return job.runAsync().join();
                }
                ResolutionResponse failureResponse = new ResolutionResponse();
                failureResponse.error = CompletableFutureUtil.getCause(err);
                return failureResponse;
            }, resolutionExecutor);
    }

    CompletableFuture<RestResponse> handleBulkJobRequest(final NodeClient client, final ObjectWriter responseWriter, final String reqBody, final Map<String, String> reqParams) {
        String[] lines = reqBody.split("\\n");
        if (lines.length % 2 != 0) {
            throw new BadRequestException("Bulk request must have repeating pairs of params and resolution body on separate lines.");
        }


        // TODO: these suppliers seem to be executing on the transport thread
        //       that might be causing blocking, so we should change this to allow passing an executor to `runParallel`
        List<Supplier<CompletableFuture<ResolutionResponse>>> runJobsSuppliers =
            Arrays.stream(lines)
                .flatMap(StreamUtil.tupleFlatmapper(new String[2]))
                .map((tuple) -> (Supplier<CompletableFuture<ResolutionResponse>>) () -> {
                    final String paramsStr = tuple[0];
                    Map<String, String> params;
                    try {
                        params = Json.toStringMap(paramsStr);
                    } catch (Exception ex) {
                        ResolutionResponse failureResponse = new ResolutionResponse();
                        failureResponse.error = new BadRequestException("Could not parse parameters: " + paramsStr);
                        return CompletableFuture.completedFuture(failureResponse);
                    }
                    final String body = tuple[1];

                    return buildAndRunJobAsync(client, body, params, reqParams);
                })
                .collect(Collectors.toList());

        int maxConcurrentJobs = config.getResolutionMaxConcurrentJobsPerRequest();

        // Start timer and begin the jobs
        final long startTime = System.nanoTime();
        // maybe this belongs better in a BulkJob class
        return CompletableFuture.supplyAsync(() -> CompletableFutureUtil.runParallel(runJobsSuppliers, maxConcurrentJobs).join(), resolutionExecutor)
            .thenApply(UnCheckedFunction.from((jobResponses) -> {
                BulkResolutionResponse response = new BulkResolutionResponse();
                // mark as an error if any of the jobs failed
                response.errors = jobResponses.stream().anyMatch(ResolutionResponse::isFailure);
                response.items = jobResponses;
                response.tookMs = Duration.ofNanos(System.nanoTime() - startTime).toMillis();

                // Jackson needs reflection access, which requires escalated security
                String responseJson = SecurityUtil.doPrivileged(
                    (CheckedSupplier<String, ?>) () -> responseWriter.writeValueAsString(response)
                );

                return new BytesRestResponse(RestStatus.OK, "application/json", responseJson);
            }));
    }

    CompletableFuture<RestResponse> handleSingleJobRequest(NodeClient client, ObjectWriter responseWriter, String body, Map<String, String> reqParams) {
        return buildAndRunJobAsync(client, body, reqParams, emptyMap())
            .thenApply(UnCheckedFunction.from((res) -> {
                // Jackson needs reflection access, which requires escalated security
                String responseJson = SecurityUtil.doPrivileged((
                    CheckedSupplier<String, ?>) () -> responseWriter.writeValueAsString(res)
                );

                RestStatus status = res.isFailure() ? RestStatus.INTERNAL_SERVER_ERROR : RestStatus.OK;

                return new BytesRestResponse(status, "application/json", responseJson);
            }));
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "_zentity/resolution"),
            new Route(POST, "_zentity/resolution/_bulk"),
            new Route(POST, "_zentity/resolution/{entity_type}"),
            new Route(POST, "_zentity/resolution/{entity_type}/_bulk")
        );
    }

    @Override
    public String getName() {
        return "zentity_resolution_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        if (!restRequest.hasContent()) {
            // Validate the request body.
            throw new BadRequestException("Request body is missing.");
        }

        // build and run all jobs, with a max concurrency for bulk jobs

        // would be better to handle incoming content as a stream for bulk,
        // but alas, unsure how to use the StreamInput format
        final String body = restRequest.content().utf8ToString();

        // Read all possible parameters into a map so that the handler knows we've consumed them
        // and all other unknowns will be thrown as unrecognized
        Map<String, String> reqParams = ParamsUtil.readAll(
            restRequest,
            PARAM_ENTITY_TYPE,
            PARAM_PRETTY,
            PARAM_INCLUDE_ATTRIBUTES,
            PARAM_INCLUDE_ERROR_TRACE,
            PARAM_INCLUDE_EXPLANATION,
            PARAM_INCLUDE_HITS,
            PARAM_INCLUDE_QUERIES,
            PARAM_INCLUDE_SCORE,
            PARAM_INCLUDE_SEQ_NO_PRIMARY_TERM,
            PARAM_INCLUDE_SOURCE,
            PARAM_INCLUDE_VERSION,
            PARAM_MAX_DOCS_PER_QUERY,
            PARAM_MAX_HOPS,
            PARAM_MAX_TIME_PER_QUERY,
            PARAM_PROFILE,
            PARAM_SEARCH_ALLOW_PARTIAL_SEARCH_RESULTS,
            PARAM_SEARCH_BATCHED_REDUCE_SIZE,
            PARAM_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS,
            PARAM_SEARCH_PRE_FILTER_SHARD_SIZE,
            PARAM_SEARCH_REQUEST_CACHE,
            PARAM_SEARCH_PREFERENCE
        );

        // Parse the request params that govern the entire request/response
        final boolean pretty = ParamsUtil.optBoolean(PARAM_PRETTY, false, reqParams, emptyMap());

        return errorHandlingConsumer(channel -> {
            final ObjectWriter writer = pretty
                ? Json.ORDERED_MAPPER.writerWithDefaultPrettyPrinter()
                : Json.MAPPER.writer();

            boolean isBulkRequest = restRequest.path().endsWith("_bulk");

            CompletableFuture<RestResponse> handleFut = isBulkRequest
                ? handleBulkJobRequest(client, writer, body, reqParams)
                : handleSingleJobRequest(client, writer, body, reqParams);

            handleFut
                .thenAccept(channel::sendResponse)
                .exceptionally(channelErrorHandler(channel));
        });
    }
}
