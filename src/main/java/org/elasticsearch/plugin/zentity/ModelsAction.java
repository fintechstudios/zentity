package org.elasticsearch.plugin.zentity;

import io.zentity.common.ActionRequestUtil;
import io.zentity.common.CompletableFutureUtil;
import io.zentity.common.FunctionalUtil.UnCheckedFunction;
import io.zentity.common.XContentUtil;
import io.zentity.model.Model;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugin.zentity.exceptions.BadRequestException;
import org.elasticsearch.plugin.zentity.exceptions.ForbiddenException;
import org.elasticsearch.plugin.zentity.exceptions.NotImplementedException;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.UnaryOperator;

import static io.zentity.common.CompletableFutureUtil.composeExceptionally;
import static org.elasticsearch.plugin.zentity.ActionUtil.errorHandlingConsumer;
import static org.elasticsearch.rest.RestRequest.Method;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class ModelsAction extends BaseZentityAction {

    public ModelsAction(ZentityConfig config) {
        super(config);
    }

    @Override
    public List<Route> routes() {
        return Arrays.asList(
            new Route(GET, "_zentity/models"),
            new Route(GET, "_zentity/models/{entity_type}"),
            new Route(POST, "_zentity/models/{entity_type}"),
            new Route(PUT, "_zentity/models/{entity_type}"),
            new Route(DELETE, "_zentity/models/{entity_type}")
        );
    }

    /**
     * Check if the .zentity-models index exists, and if it doesn't, then create it.
     *
     * @param client The client that will communicate with Elasticsearch.
     */
    CompletableFuture<Void> ensureIndex(NodeClient client) {
        IndicesExistsRequestBuilder request = client.admin()
            .indices()
            .prepareExists(config.getModelsIndexName());

        return ActionRequestUtil.toCompletableFuture(request)
            .thenCompose((res) -> {
                if (!res.isExists()) {
                    return new SetupAction(config).createIndex(client).thenApply((val) -> null);
                }
                return CompletableFuture.completedFuture(null);
            });
    }

    /**
     * Run an {@link ActionRequest} that implicitly creates the .zentity-models index if it does
     * not already exist.
     *
     * @param client The client that will communicate with Elasticsearch.
     * @param <ReqT> The type of Request.
     * @param <ResT> The type of Response.
     * @return The response from Elasticsearch.
     * @throws ForbiddenException If the user is not authorized to create the .zentity-models index.
     */
    <ReqT extends ActionRequest, ResT extends ActionResponse> CompletableFuture<ResT>
    getResponseWithImplicitIndexCreation(NodeClient client, ActionRequestBuilder<ReqT, ResT> builder) {
        return composeExceptionally(
            ActionRequestUtil.toCompletableFuture(builder),
            (ex) -> {
                Throwable cause = CompletableFutureUtil.getCause(ex);
                if (!(cause instanceof IndexNotFoundException)) {
                    throw new CompletionException(cause);
                }
                return new SetupAction(config)
                    .createIndex(client)
                    .thenCompose(res -> ActionRequestUtil.toCompletableFuture(builder));
            }
        );
    }

    /**
     * Retrieve all entity models.
     *
     * @param client The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    CompletableFuture<SearchResponse> listEntityModels(NodeClient client) {
        SearchRequestBuilder request = client.prepareSearch(config.getModelsIndexName());
        request.setSize(10000); // max request size
        return getResponseWithImplicitIndexCreation(client, request);
    }

    /**
     * Retrieve one entity model by its type.
     *
     * @param entityType The entity type.
     * @param client     The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    CompletableFuture<GetResponse> getEntityModel(String entityType, NodeClient client) {
        GetRequestBuilder request = client.prepareGet(config.getModelsIndexName(), "doc", entityType);
        return getResponseWithImplicitIndexCreation(client, request);
    }

    /**
     * Index one entity model by its type. Return error if an entity model already exists for that entity type.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    CompletableFuture<IndexResponse> indexEntityModel(String entityType, String requestBody, NodeClient client) {
        return ensureIndex(client)
            .thenCompose((nil) -> {
                IndexRequestBuilder request = client.prepareIndex(config.getModelsIndexName(), "doc", entityType);
                request.setSource(requestBody, XContentType.JSON).setCreate(true).setRefreshPolicy("wait_for");
                return ActionRequestUtil.toCompletableFuture(request);
            });
    }

    /**
     * Update one entity model by its type. Does not support partial updates.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    CompletableFuture<IndexResponse> updateEntityModel(String entityType, String requestBody, NodeClient client) {
        return ensureIndex(client)
            .thenCompose((nil) -> {
                IndexRequestBuilder request = client.prepareIndex(config.getModelsIndexName(), "doc", entityType);
                request
                    .setSource(requestBody, XContentType.JSON)
                    .setCreate(false)
                    .setRefreshPolicy("wait_for");
                return ActionRequestUtil.toCompletableFuture(request);
            });
    }

    /**
     * Delete one entity model by its type.
     *
     * @param entityType The entity type.
     * @param client     The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    CompletableFuture<DeleteResponse> deleteEntityModel(String entityType, NodeClient client) {
        DeleteRequestBuilder request = client.prepareDelete(config.getModelsIndexName(), "doc", entityType);
        request.setRefreshPolicy("wait_for");
        return getResponseWithImplicitIndexCreation(client, request);
    }

    @Override
    public String getName() {
        return "zentity_models_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        // Allow access the model system index
        client.threadPool().getThreadContext().markAsSystemContext();

        // Parse request
        String entityType = restRequest.param("entity_type");
        boolean pretty = restRequest.paramAsBoolean("pretty", false);
        Method method = restRequest.method();
        String requestBody = restRequest.content().utf8ToString();

        final UnaryOperator<XContentBuilder> prettyPrintModifier = pretty
            ? XContentBuilder::prettyPrint
            : UnaryOperator.identity();

        return errorHandlingConsumer(channel -> {
            // Validate input
            if (method == POST || method == PUT) {

                // Parse the request body.
                if (requestBody == null || requestBody.equals("")) {
                    throw new BadRequestException("Request body is missing.");
                }

                // Parse and validate the entity model.
                new Model(requestBody);
            }

            final CompletableFuture<? extends ToXContent> responseFuture;

            // Handle request
            if (method == GET && (entityType == null || entityType.equals(""))) {
                // GET _zentity/models
                responseFuture = listEntityModels(client);
            } else if (method == GET) {
                // GET _zentity/models/{entity_type}
                responseFuture = getEntityModel(entityType, client);
            } else if (method == POST && !entityType.equals("")) {
                // POST _zentity/models/{entity_type}
                responseFuture = indexEntityModel(entityType, requestBody, client);
            } else if (method == PUT && !entityType.equals("")) {
                // PUT _zentity/models/{entity_type}
                responseFuture = updateEntityModel(entityType, requestBody, client);
            } else if (method == DELETE && !entityType.equals("")) {
                // DELETE _zentity/models/{entity_type}
                responseFuture = deleteEntityModel(entityType, client);
            } else {
                throw new NotImplementedException("Method and endpoint not implemented.");
            }

            responseFuture
                .thenApply(UnCheckedFunction.from(res -> res.toXContent(XContentUtil.jsonBuilder(prettyPrintModifier), ToXContent.EMPTY_PARAMS)))
                .thenAccept((builder) -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder)))
                .get();
        });
    }
}
