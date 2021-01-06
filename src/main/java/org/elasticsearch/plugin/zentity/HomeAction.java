package org.elasticsearch.plugin.zentity;

import io.zentity.common.XContentUtils;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.UnaryOperator;

import static org.elasticsearch.plugin.zentity.ActionUtil.errorHandlingConsumer;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class HomeAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(GET, "_zentity")
        );
    }

    @Override
    public String getName() {
        return "zentity_plugin_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        final Properties props = ZentityPlugin.properties();
        final boolean pretty = restRequest.paramAsBoolean("pretty", false);

        final UnaryOperator<XContentBuilder> prettyPrintModifier = (builder) -> {
            if (pretty) {
                return builder.prettyPrint();
            }
            return builder;
        };

        final UnaryOperator<XContentBuilder> propsResponseModifier = XContentUtils.uncheckedModifier((builder) -> {
            builder.startObject();

            builder.field("name", props.getProperty("name"));
            builder.field("description", props.getProperty("description"));
            builder.field("website", props.getProperty("zentity.website"));

            builder.startObject("version");
            builder.field("zentity", props.getProperty("zentity.version"));
            builder.field("elasticsearch", props.getProperty("elasticsearch.version"));
            builder.endObject();

            builder.endObject();

            return builder;
        });

        final UnaryOperator<XContentBuilder> composedModifier = XContentUtils.composeModifiers(
            Arrays.asList(
                prettyPrintModifier,
                propsResponseModifier
            )
        );

        return errorHandlingConsumer(channel -> {
            XContentBuilder contentBuilder = XContentUtils.jsonBuilder(composedModifier);
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, contentBuilder));
        });
    }
}
