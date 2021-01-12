package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import static io.zentity.devtools.JsonTestUtil.assertUnorderedEquals;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ModelsActionIT extends AbstractActionITCase {
    @Test
    public void testGetUnknownModel() throws Exception {
        Response response = client.performRequest(new Request("GET", "_zentity/models/unknown"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());

        JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

        assertTrue("_id field present", json.has("_id"));
        assertEquals("unknown", json.get("_id").textValue());

        assertTrue("_type field present", json.has("_type"));
        assertEquals("doc", json.get("_type").textValue());

        assertTrue("found field present", json.has("found"));
        assertFalse(json.get("found").booleanValue());
    }

    @Test
    public void testListModels() throws Exception {
        prepareTestEntityModelA();
        prepareTestEntityModelB();
        try {
            Response response = client.performRequest(new Request("GET", "_zentity/models"));
            assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());

            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            assertTrue("took field present", json.has("took"));
            assertTrue("_shards field present", json.has("_shards"));

            assertTrue("timed_out field present", json.has("timed_out"));
            assertFalse("timed_out is false", json.get("timed_out").booleanValue());

            assertTrue("hits field present", json.has("hits"));

            JsonNode hitsWrapper = json.get("hits");
            assertTrue("hits.hits field present", hitsWrapper.has("hits"));

            JsonNode hits = hitsWrapper.get("hits");
            assertTrue(hits.isArray());
            assertEquals(2, hits.size());
        } finally {
            destroyTestEntityModelA();
            destroyTestEntityModelB();
        }
    }

    @Test
    public void testGetKnownModel() throws Exception {
        JsonNode expectedModel = Json.ORDERED_MAPPER.readTree(getTestEntityModelAJson());
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            Response response = client.performRequest(new Request("GET", "_zentity/models/zentity_test_entity_a"));
            assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());

            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            assertTrue("_id field present", json.has("_id"));
            assertEquals("zentity_test_entity_a", json.get("_id").textValue());

            assertTrue("_type field present", json.has("_type"));
            assertEquals("doc", json.get("_type").textValue());

            assertTrue("found field present", json.has("found"));
            assertTrue(json.get("found").booleanValue());

            assertTrue("_source field present", json.has("_source"));
            assertUnorderedEquals(expectedModel, json.get("_source"));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testDeleteUnknown() throws Exception {
        Response response = client.performRequest(new Request("DELETE", "_zentity/models/zentity_test_entity_a"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());

        JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

        assertTrue("_id field present", json.has("_id"));
        assertEquals("zentity_test_entity_a", json.get("_id").textValue());

        assertTrue("_type field present", json.has("_type"));
        assertEquals("doc", json.get("_type").textValue());

        assertTrue("result field present", json.has("result"));
        assertEquals("not_found", json.get("result").textValue());
    }

    @Test
    public void testDeleteModel() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            Response response = client.performRequest(new Request("DELETE", "_zentity/models/zentity_test_entity_a"));
            assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());

            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            assertTrue("_id field present", json.has("_id"));
            assertEquals("zentity_test_entity_a", json.get("_id").textValue());

            assertTrue("_type field present", json.has("_type"));
            assertEquals("doc", json.get("_type").textValue());

            assertTrue("result field present", json.has("result"));
            assertEquals("deleted", json.get("result").textValue());
        } finally {
            destroyTestResources(testResourceSet);
        }
    }
}
