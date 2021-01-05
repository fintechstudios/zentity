package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.devtools.AbstractITCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ModelsActionIT extends AbstractITCase {
    @Test
    public void testGetUnknownModel() throws Exception {
        Response response = client.performRequest(new Request("GET", "_zentity/models/unknown"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());

        JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());

        assertTrue("_id field present", json.has("_id"));
        assertEquals("unknown", json.get("_id").textValue());

        assertTrue("found field present", json.has("found"));
        assertFalse(json.get("found").booleanValue());
    }
}
