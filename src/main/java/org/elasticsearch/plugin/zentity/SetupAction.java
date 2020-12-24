package org.elasticsearch.plugin.zentity;

import io.zentity.common.ActionRequestUtil;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugin.zentity.exceptions.ForbiddenException;
import org.elasticsearch.plugin.zentity.exceptions.NotImplementedException;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.zentity.common.CompletableFutureUtil.checkedConsumer;
import static org.elasticsearch.rest.RestRequest.Method;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class SetupAction extends BaseAction {

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
    public static final String INDEX_MAPPING_ELASTICSEARCH_6 = "{\n" +
        "  \"doc\": " + INDEX_MAPPING + "\n" +
        "}";

    @Inject
    public SetupAction(RestController controller) {
        controller.registerHandler(POST, "_zentity/_setup", this);
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
        Properties props = ZentityPlugin.properties();

        CreateIndexRequestBuilder reqBuilder = client.admin().indices().prepareCreate(ModelsAction.INDEX_NAME)
            .setSettings(Settings.builder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", numberOfReplicas)
            );

        if (props.getProperty("elasticsearch.version").compareTo("7.") >= 0) {
            reqBuilder
                .addMapping("doc", INDEX_MAPPING, XContentType.JSON);
        } else {
            reqBuilder
                .addMapping("doc", INDEX_MAPPING_ELASTICSEARCH_6, XContentType.JSON);
        }

        return ActionRequestUtil.toCompletableFuture(reqBuilder)
            .exceptionally(ex -> {
                Throwable toThrow = ex;
                if (ex instanceof ElasticsearchSecurityException) {
                    toThrow = new ForbiddenException("The .zentity-models index does not exist and you do not have the 'create_index' privilege. An authorized user must create the index by submitting: POST _zentity/_setup");
                    toThrow.initCause(ex);
                }
                throw new CompletionException(toThrow);
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
        boolean pretty = restRequest.paramAsBoolean("pretty", false);
        int numberOfShards = restRequest.paramAsInt("number_of_shards", 1);
        int numberOfReplicas = restRequest.paramAsInt("number_of_replicas", 1);
        Method method = restRequest.method();

        return errorHandlingConsumer(channel -> {
            if (method == POST) {
                createIndex(client, numberOfShards, numberOfReplicas)
                    .thenAccept(checkedConsumer((createRes) -> {
                        XContentBuilder content = XContentFactory.jsonBuilder();
                        if (pretty) {
                            content.prettyPrint();
                        }
                        content.startObject().field("acknowledged", true).endObject();
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, content));
                    }))
                    .get();

            } else {
                throw new NotImplementedException("Method and endpoint not implemented.");
            }
        });
    }
}
