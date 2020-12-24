package org.elasticsearch.plugin.zentity;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Properties;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class HomeAction extends BaseAction {

    @Inject
    public HomeAction(RestController controller) {
        controller.registerHandler(GET, "_zentity", this);
    }

    @Override
    public String getName() {
        return "zentity_plugin_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        Properties props = ZentityPlugin.properties();
        boolean pretty = restRequest.paramAsBoolean("pretty", false);

        return errorHandlingConsumer(channel -> {
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            if (pretty) {
                contentBuilder.prettyPrint();
            }
            contentBuilder.startObject();
            contentBuilder.field("name", props.getProperty("name"));
            contentBuilder.field("description", props.getProperty("description"));
            contentBuilder.field("website", props.getProperty("zentity.website"));

            contentBuilder.startObject("version");
            contentBuilder.field("zentity", props.getProperty("zentity.version"));
            contentBuilder.field("elasticsearch", props.getProperty("elasticsearch.version"));
            contentBuilder.endObject();

            contentBuilder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, contentBuilder));
        });
    }
}
