package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.devtools.AbstractITCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

public class SetupActionIT extends AbstractITCase {
    @Test
    public void testSetup() throws Exception {
        Response response = client.performRequest(new Request("POST", "_zentity/_setup"));
        JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());

        assertTrue("should be acknowledged", json.has("acknowledged") && json.get("acknowledged").asBoolean());

        try {
            Response getIndexRes = client.performRequest(new Request("GET", ".zentity-models"));
            JsonNode getResJson = Json.MAPPER.readTree(getIndexRes.getEntity().getContent());

            assertTrue("found index", getResJson.has(".zentity-models"));

            JsonNode indexSettings = getResJson.at("/.zentity-models/settings/index");
            assertEquals("default # of shards", 1, indexSettings.get("number_of_shards").asInt());
            assertEquals("default # of replicas", 1, indexSettings.get("number_of_replicas").asInt());
        } finally {
            client.performRequest(new Request("DELETE", ".zentity-models"));
        }
    }

    @Test
    public void testUninstall() throws Exception {
        client.performRequest(new Request("POST", "_zentity/_setup"));

        Response response = client.performRequest(new Request("DELETE", "_zentity/_setup"));
        JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());

        assertTrue("should be acknowledged", json.has("acknowledged") && json.get("acknowledged").asBoolean());

        try {
            client.performRequest(new Request("GET", ".zentity-models"));
            fail("expected failure");
        } catch (ResponseException ex) {
            assertEquals("index not found", 404, ex.getResponse().getStatusLine().getStatusCode());
        }
    }
}
