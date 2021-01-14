package org.elasticsearch.plugin.zentity;

import io.zentity.common.FunctionalUtil.UnCheckedUnaryOperator;
import io.zentity.common.XContentUtil;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.List;
import java.util.Properties;
import java.util.function.UnaryOperator;

import static org.elasticsearch.plugin.zentity.ActionUtil.errorHandlingConsumer;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.PATCH;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class HomeAction extends BaseZentityAction {
    public HomeAction(ZentityConfig config) {
        super(config);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "_zentity"),
            new DeprecatedRoute(POST, "_zentity", "Only GET requests will be supported in future versions"),
            new DeprecatedRoute(PUT, "_zentity", "Only GET requests will be supported in future versions"),
            new DeprecatedRoute(DELETE, "_zentity", "Only GET requests will be supported in future versions"),
            new DeprecatedRoute(PATCH, "_zentity", "Only GET requests will be supported in future versions")
        );
    }

    @Override
    public String getName() {
        return "zentity_plugin_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        final boolean pretty = restRequest.paramAsBoolean("pretty", false);

        final UnaryOperator<XContentBuilder> prettyPrintModifier = (builder) -> {
            if (pretty) {
                return builder.prettyPrint();
            }
            return builder;
        };

        final UnaryOperator<XContentBuilder> propsResponseModifier = UnCheckedUnaryOperator.from((builder) -> {
            builder.startObject();

            final Properties props = ZentityPlugin.properties();

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

        final UnaryOperator<XContentBuilder> responseBuilderFunc = XContentUtil.composeModifiers(
            List.of(prettyPrintModifier, propsResponseModifier)
        );

        return errorHandlingConsumer(channel -> {
            XContentBuilder contentBuilder = XContentUtil.jsonBuilder(responseBuilderFunc);
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, contentBuilder));
        });
    }
}
