package org.elasticsearch.plugin.zentity;

import io.zentity.common.ActionRequestUtil;
import io.zentity.common.CompletableFutureUtil;
import io.zentity.common.FunctionalUtil.UnCheckedFunction;
import io.zentity.common.FunctionalUtil.UnCheckedUnaryOperator;
import io.zentity.common.XContentUtil;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugin.zentity.exceptions.ForbiddenException;
import org.elasticsearch.plugin.zentity.exceptions.NotImplementedException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.UnaryOperator;

import static org.elasticsearch.plugin.zentity.ActionUtil.errorHandlingConsumer;
import static org.elasticsearch.rest.RestRequest.Method;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class SetupAction extends BaseRestHandler {

    public static final int DEFAULT_NUMBER_OF_SHARDS = 1;
    public static final int DEFAULT_NUMBER_OF_REPLICAS = 1;
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

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(POST, "_zentity/_setup"));
    }

    /**
     * Create the .zentity-models index.
     *
     * @param client           The client that will communicate with Elasticsearch.
     * @param numberOfShards   The value of index.number_of_shards.
     * @param numberOfReplicas The value of index.number_of_replicas.
     * @return A completable future that completes when the index is created.
     */
    public static CompletableFuture<CreateIndexResponse> createIndex(NodeClient client, int numberOfShards, int numberOfReplicas) {
        // Elasticsearch 7.0.0+ removes mapping types
        CreateIndexRequestBuilder reqBuilder = client
            .admin()
            .indices()
            .prepareCreate(ModelsAction.INDEX_NAME)
            .addMapping("doc", INDEX_MAPPING, XContentType.JSON)
            .setSettings(Settings.builder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", numberOfReplicas)
            );

        return ActionRequestUtil.toCompletableFuture(reqBuilder)
            .exceptionally(ex -> {
                Throwable cause = CompletableFutureUtil.getCause(ex);
                if (cause instanceof ElasticsearchSecurityException) {
                    cause = new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
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
    public static CompletableFuture<CreateIndexResponse> createIndex(NodeClient client) {
        return createIndex(client, DEFAULT_NUMBER_OF_SHARDS, DEFAULT_NUMBER_OF_REPLICAS);
    }

    @Override
    public String getName() {
        return "zentity_setup_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        // Parse request
        final boolean pretty = restRequest.paramAsBoolean("pretty", false);
        final int numberOfShards = restRequest.paramAsInt("number_of_shards", 1);
        final int numberOfReplicas = restRequest.paramAsInt("number_of_replicas", 1);
        final Method method = restRequest.method();

        final UnaryOperator<XContentBuilder> prettyPrintModifier = (builder) -> {
            if (pretty) {
                return builder.prettyPrint();
            }
            return builder;
        };

        final UnaryOperator<XContentBuilder> ackResponseModifier = UnCheckedUnaryOperator.from(
            (builder) -> builder
                .startObject()
                .field("acknowledged", true)
                .endObject()
        );

        final UnaryOperator<XContentBuilder> composedModifier = XContentUtil.composeModifiers(
            Arrays.asList(
                prettyPrintModifier,
                ackResponseModifier
            )
        );

        return errorHandlingConsumer(channel -> {
            if (method == POST) {
                createIndex(client, numberOfShards, numberOfReplicas)
                    .thenApply(UnCheckedFunction.from(res -> XContentUtil.jsonBuilder(composedModifier)))
                    .thenAccept(builder -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder)))
                    .get();

            } else {
                throw new NotImplementedException("Method and endpoint not implemented.");
            }
        });
    }
}
