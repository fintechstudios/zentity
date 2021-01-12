package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.zentity.common.Json;
import joptsimple.internal.Strings;
import org.apache.http.Consts;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResolutionActionBulkIT extends AbstractActionITCase {

    private static final ContentType NDJSON_TYPE = ContentType.create("application/x-ndjson", Consts.UTF_8);

    private static final String TEST_PAYLOAD_JOB_TERMS_JSON = "{" +
        "  \"terms\": [ \"a_00\" ]," +
        "  \"scope\": {" +
        "    \"include\": {" +
        "      \"indices\": [ \"zentity_test_index_a\", \"zentity_test_index_b\", \"zentity_test_index_c\" ]," +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\" ]" +
        "    }" +
        "  }" +
        "}";

    private static final String TEST_PAYLOAD_JOB_EXPLANATION_JSON = "{" +
        "  \"attributes\": {" +
        "    \"attribute_a\": [ \"a_00\" ]," +
        "    \"attribute_type_date\": {" +
        "      \"values\": [ \"1999-12-31T23:59:57.0000\" ]," +
        "      \"params\": {" +
        "        \"format\" : \"yyyy-MM-dd'T'HH:mm:ss.0000\"," +
        "        \"window\" : \"1d\"" +
        "      }" +
        "    }" +
        "  }," +
        "  \"scope\": {" +
        "    \"include\": {" +
        "      \"indices\": [ \"zentity_test_index_a\" ]" +
        "    }" +
        "  }" +
        "}";

    @Test
    public void testBulkResolution() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a/_bulk";
            Request req = new Request("POST", endpoint);
            String[] reqBodyLines = new String[]{
                "{\"_source\": false}", // override source
                TEST_PAYLOAD_JOB_TERMS_JSON,
                "{\"_explanation\": true}", // override explanation
                TEST_PAYLOAD_JOB_EXPLANATION_JSON
            };
            String reqBody = Strings.join(reqBodyLines, "\n");
            req.setEntity(new NStringEntity(reqBody, NDJSON_TYPE));
            req.addParameter("_explanation", "false");
            req.addParameter("_source", "true");

            Response response = client.performRequest(req);
            assertEquals(response.getStatusLine().getStatusCode(), 200);

            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            // check shape
            assertTrue(json.isObject());
            assertTrue(json.has("errors"));
            assertTrue(json.get("errors").isBoolean());

            assertTrue(json.has("took"));
            assertTrue(json.get("took").isNumber());

            assertTrue(json.has("items"));
            assertTrue(json.get("items").isArray());

            // check the values
            assertFalse(json.get("errors").booleanValue());
            assertTrue(json.get("took").asLong() > 0);

            ArrayNode items = (ArrayNode) json.get("items");
            assertEquals(2, items.size());
            items.forEach((item) -> {
                assertTrue(item.has("hits"));
                assertTrue(item.get("hits").isObject());

                JsonNode hits = item.get("hits");
                assertTrue(hits.has("hits"));
                assertTrue(hits.get("hits").isArray());

                assertTrue(item.has("took"));
                assertTrue(item.get("took").isNumber());
            });

            JsonNode termsResult = items.get(0);
            assertTrue(termsResult.get("hits").get("total").asInt() > 0);

            JsonNode firstTermHit = termsResult.get("hits").get("hits").get(0);
            assertFalse(firstTermHit.has("_source"));
            assertFalse(firstTermHit.has("_explanation"));

            JsonNode explanationResult = items.get(1);
            assertTrue(explanationResult.get("hits").get("total").asInt() > 0);

            JsonNode firstExplanationHit = explanationResult.get("hits").get("hits").get(0);
            assertTrue(firstExplanationHit.has("_source"));
            assertTrue(firstExplanationHit.has("_explanation"));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }
}
