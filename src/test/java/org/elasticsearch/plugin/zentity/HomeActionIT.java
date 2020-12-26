package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.devtools.AbstractITCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class HomeActionIT extends AbstractITCase {
    @Test
    public void testResponseInfo() throws Exception {
        Response response = client.performRequest(new Request("GET", "_zentity"));
        JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());

        Properties properties = ZentityPlugin.properties();

        assertTrue("should have a name", json.has("name"));
        assertEquals(properties.get("name"), json.get("name").asText());

        assertTrue("should have a description", json.has("description"));
        assertEquals(properties.get("description"), json.get("description").asText());

        assertTrue("should have a website", json.has("website"));
        assertEquals(properties.get("zentity.website"), json.get("website").asText());

        assertTrue("should have a version object", json.has("version"));
        JsonNode versionObj = json.get("version");
        assertEquals(properties.get("zentity.version"), versionObj.get("zentity").asText());
        assertEquals(properties.get("elasticsearch.version"), versionObj.get("elasticsearch").asText());
    }
}
