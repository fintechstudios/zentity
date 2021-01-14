package org.elasticsearch.plugin.zentity;

import io.zentity.common.ActionRequestUtil;
import io.zentity.common.CompletableFutureUtil;
import io.zentity.common.FunctionalUtil;
import io.zentity.common.FunctionalUtil.UnCheckedUnaryOperator;
import io.zentity.common.XContentUtil;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugin.zentity.exceptions.ForbiddenException;
import org.elasticsearch.plugin.zentity.exceptions.NotFoundException;
import org.elasticsearch.plugin.zentity.exceptions.NotImplementedException;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.UnaryOperator;

import static org.elasticsearch.plugin.zentity.ActionUtil.errorHandlingConsumer;
import static org.elasticsearch.rest.RestRequest.Method;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class SetupAction extends BaseZentityAction {

    public static final String INDEX_MAPPING = "{\n" +
        "  \"dynamic\": \"strict\",\n" +
        "  \"properties\": {\n" +
        "    \"attributes\": {\n" +
        "      \"type\": \"object\",\n" +
        "      \"enabled\": false\n" +
        "    },\n" +
        "    \"resolvers\": {\n" +
        "      \"type\": \"object\",\n" +
        "      \"enabled\": false\n" +
        "    },\n" +
        "    \"matchers\": {\n" +
        "      \"type\": \"object\",\n" +
        "      \"enabled\": false\n" +
        "    },\n" +
        "    \"indices\": {\n" +
        "      \"type\": \"object\",\n" +
        "      \"enabled\": false\n" +
        "    }\n" +
        "  }\n" +
        "}";

    public SetupAction(ZentityConfig config) {
        super(config);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "_zentity/_setup"),
            new Route(DELETE, "_zentity/_setup")
        );
    }

    /**
     * Create the zentity models index.
     *
     * @param client           The client that will communicate with Elasticsearch.
     * @param numberOfShards   The value of index.number_of_shards.
     * @param numberOfReplicas The value of index.number_of_replicas.
     * @return A completable future that completes when the index is created.
     */
    CompletableFuture<CreateIndexResponse> createIndex(NodeClient client, int numberOfShards, int numberOfReplicas) {
        CreateIndexRequestBuilder reqBuilder = client
            .admin()
            .indices()
            .prepareCreate(config.getModelsIndexName())
            .addMapping("doc", INDEX_MAPPING, XContentType.JSON)
            .setSettings(Settings.builder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", numberOfReplicas)
            );

        return ActionRequestUtil.toCompletableFuture(reqBuilder)
            .exceptionally(ex -> {
                Throwable cause = CompletableFutureUtil.getCause(ex);
                if (cause instanceof ElasticsearchSecurityException) {
                    cause = new ForbiddenException("The " + config.getModelsIndexName() + " index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
                    cause.initCause(ex);
                }
                throw new CompletionException(cause);
            });
    }

    /**
     * Create the .zentity-models index using the default index settings.
     *
     * @param client The client that will communicate with Elasticsearch.
     * @return A completable future that completes when the index is created.
     */
    public CompletableFuture<CreateIndexResponse> createIndex(NodeClient client) {
        return createIndex(client, config.getModelsIndexDefaultNumberOfShards(), config.getModelsIndexDefaultNumberOfReplicas());
    }

    public CompletableFuture<AcknowledgedResponse> deleteIndex(NodeClient client) {
        DeleteIndexRequestBuilder reqBuilder = client
            .admin()
            .indices()
            .prepareDelete(config.getModelsIndexName());

        return ActionRequestUtil.toCompletableFuture(reqBuilder)
            .exceptionally(ex -> {
                Throwable cause = CompletableFutureUtil.getCause(ex);
                if (cause instanceof ElasticsearchSecurityException) {
                    String message = "You do not have the 'delete_index' privilege for the "
                        + config.getModelsIndexName() + "index." +
                        " An authorized user must delete the index by submitting: DELETE _zentity/_setup";
                    cause = new ForbiddenException(message, cause);
                } else if (cause instanceof IndexNotFoundException) {
                    cause = new NotFoundException("Zentity has not been set up. Please send a POST _zentity/_setup request.", cause);
                }
                throw new CompletionException(cause);
            });
    }

    @Override
    public String getName() {
        return "zentity_setup_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        // Parse request
        final boolean pretty = restRequest.paramAsBoolean("pretty", false);
        final int numberOfShards = restRequest.paramAsInt("number_of_shards", config.getModelsIndexDefaultNumberOfShards());
        final int numberOfReplicas = restRequest.paramAsInt("number_of_replicas", config.getModelsIndexDefaultNumberOfReplicas());
        final Method method = restRequest.method();

        final UnaryOperator<XContentBuilder> prettyPrintModifier = (builder) -> {
            if (pretty) {
                return builder.prettyPrint();
            }
            return builder;
        };

        final UnaryOperator<XContentBuilder> ackResponseModifier = UnCheckedUnaryOperator.from(
            (builder) -> new AcknowledgedResponse(true).toXContent(builder, ToXContent.EMPTY_PARAMS)
        );

        final UnaryOperator<XContentBuilder> responseBuilderFunc = XContentUtil.composeModifiers(
            List.of(prettyPrintModifier, ackResponseModifier)
        );

        return errorHandlingConsumer(channel -> {
            final CompletableFuture<?> fut;
            if (method == POST) {
                fut = createIndex(client, numberOfShards, numberOfReplicas);
            } else if (method == DELETE) {
                fut = deleteIndex(client);
            } else {
                throw new NotImplementedException("Method and endpoint not implemented.");
            }

            fut.thenApply(FunctionalUtil.UnCheckedFunction.from(res -> XContentUtil.jsonBuilder(responseBuilderFunc)))
                .thenAccept(builder -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder)))
                .get();
        });
    }
}
