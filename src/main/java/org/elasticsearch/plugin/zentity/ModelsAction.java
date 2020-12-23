package org.elasticsearch.plugin.zentity;

import io.zentity.model.Model;
import io.zentity.resolution.XContentUtils;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugin.zentity.exceptions.BadRequestException;
import org.elasticsearch.plugin.zentity.exceptions.ForbiddenException;
import org.elasticsearch.plugin.zentity.exceptions.NotImplementedException;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.elasticsearch.rest.RestRequest.Method;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class ModelsAction extends BaseAction {

    public static final String INDEX_NAME = ".zentity-models";

    @Inject
    public ModelsAction(RestController controller) {
        controller.registerHandler(GET, "_zentity/models", this);
        controller.registerHandler(GET, "_zentity/models/{entity_type}", this);
        controller.registerHandler(POST, "_zentity/models/{entity_type}", this);
        controller.registerHandler(PUT, "_zentity/models/{entity_type}", this);
        controller.registerHandler(DELETE, "_zentity/models/{entity_type}", this);
    }

    /**
     * Check if the .zentity-models index exists, and if it doesn't, then create it.
     *
     * @param client The client that will communicate with Elasticsearch.
     * @throws ForbiddenException
     */
    static void ensureIndex(NodeClient client) throws ForbiddenException {
        IndicesExistsRequestBuilder request = client.admin().indices().prepareExists(INDEX_NAME);
        IndicesExistsResponse response = getResponseWithImplicitIndexCreation(client, request);
        if (!response.isExists()) {
            SetupAction.createIndex(client);
        }
    }

    /**
     *
     * @param client The client that will communicate with Elasticsearch.
     * @param <ReqT> The type of Request.
     * @param <ResT> The type of Response.
     * @return The response from Elasticsearch.
     * @throws ForbiddenException If the user is not authorized to create the .zentity-models index.
     */
    static <ReqT extends ActionRequest, ResT extends ActionResponse> ResT getResponseWithImplicitIndexCreation(NodeClient client, ActionRequestBuilder<ReqT, ResT> builder) {
        try {
            // TODO: better future handling
            return builder.get();
        } catch (IndexNotFoundException e) {
            try {
                SetupAction.createIndex(client);
            } catch (ElasticsearchSecurityException se) {
                throw new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
            }
            return builder.get();
        }
    }

    /**
     * Retrieve all entity models.
     *
     * @param client The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     */
    static SearchResponse getEntityModels(NodeClient client) throws ForbiddenException {
        SearchRequestBuilder request = client.prepareSearch(INDEX_NAME);
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
    static GetResponse getEntityModel(String entityType, NodeClient client) {
        GetRequestBuilder request = client.prepareGet(INDEX_NAME, "doc", entityType);
        return getResponseWithImplicitIndexCreation(client, request);
    }

    /**
     * Index one entity model by its type. Return error if an entity model already exists for that entity type.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     * @throws ForbiddenException
     */
    static IndexResponse indexEntityModel(String entityType, String requestBody, NodeClient client) throws ForbiddenException {
        ensureIndex(client);
        IndexRequestBuilder request = client.prepareIndex(INDEX_NAME, "doc", entityType);
        request.setSource(requestBody, XContentType.JSON).setCreate(true).setRefreshPolicy("wait_for");
        return request.get();
    }

    /**
     * Update one entity model by its type. Does not support partial updates.
     *
     * @param entityType  The entity type.
     * @param requestBody The request body.
     * @param client      The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     * @throws ForbiddenException
     */
    static IndexResponse updateEntityModel(String entityType, String requestBody, NodeClient client) throws ForbiddenException {
        ensureIndex(client);
        IndexRequestBuilder request = client.prepareIndex(INDEX_NAME, "doc", entityType);
        request.setSource(requestBody, XContentType.JSON).setCreate(false).setRefreshPolicy("wait_for");
        return request.get();
    }

    /**
     * Delete one entity model by its type.
     *
     * @param entityType The entity type.
     * @param client     The client that will communicate with Elasticsearch.
     * @return The response from Elasticsearch.
     * @throws ForbiddenException
     */
    static DeleteResponse deleteEntityModel(String entityType, NodeClient client) throws ForbiddenException {
        DeleteRequestBuilder request = client.prepareDelete(INDEX_NAME, "doc", entityType);
        request.setRefreshPolicy("wait_for");
        return getResponseWithImplicitIndexCreation(client, request);
    }

    @Override
    public String getName() {
        return "zentity_models_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        // Parse request
        String entityType = restRequest.param("entity_type");
        boolean pretty = restRequest.paramAsBoolean("pretty", false);
        Method method = restRequest.method();
        String requestBody = restRequest.content().utf8ToString();

        UnaryOperator<XContentBuilder> prettyPrintModifier = (builder) -> {
            if (pretty) {
                return builder.prettyPrint();
            }
            return builder;
        };

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

            // Handle request
            if (method == GET && (entityType == null || entityType.equals(""))) {
                // GET _zentity/models
                SearchResponse response = getEntityModels(client);
                String responseBody = XContentUtils.serializeAsJson(prettyPrintModifier, response);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, responseBody));
            } else if (method == GET) {
                // GET _zentity/models/{entity_type}
                GetResponse response = getEntityModel(entityType, client);
                String responseBody = XContentUtils.serializeAsJson(prettyPrintModifier, response);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, responseBody));

            } else if (method == POST && !entityType.equals("")) {
                // POST _zentity/models/{entity_type}
                IndexResponse response = indexEntityModel(entityType, requestBody, client);
                String responseBody = XContentUtils.serializeAsJson(prettyPrintModifier, response);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, responseBody));

            } else if (method == PUT && !entityType.equals("")) {
                // PUT _zentity/models/{entity_type}
                IndexResponse response = updateEntityModel(entityType, requestBody, client);
                String responseBody = XContentUtils.serializeAsJson(prettyPrintModifier, response);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, responseBody));

            } else if (method == DELETE && !entityType.equals("")) {
                // DELETE _zentity/models/{entity_type}
                DeleteResponse response = deleteEntityModel(entityType, client);
                String responseBody = XContentUtils.serializeAsJson(prettyPrintModifier, response);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, responseBody));

            } else {
                throw new NotImplementedException("Method and endpoint not implemented.");
            }
        });
    }
}
