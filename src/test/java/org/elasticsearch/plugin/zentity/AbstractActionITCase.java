package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.devtools.AbstractITCase;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public abstract class AbstractActionITCase extends AbstractITCase {
    protected final int TEST_RESOURCES_A = 0;
    protected final int TEST_RESOURCES_B = 1;
    protected final int TEST_RESOURCES_ARRAYS = 3;
    protected final int TEST_RESOURCES_ELASTICSEARCH_ERROR = 4;
    protected final int TEST_RESOURCES_ZENTITY_ERROR = 5;

    private byte[] readFile(String filename) throws IOException {
        InputStream stream = this.getClass().getResourceAsStream("/" + filename);
        return IOUtils.toByteArray(stream);
    }

    private void destroyTestIndices(int testResourceSet) throws IOException {
        switch (testResourceSet) {
            case TEST_RESOURCES_ARRAYS:
                client.performRequest(new Request("DELETE", "zentity_test_index_arrays"));
                break;
            default:
                client.performRequest(new Request("DELETE", "zentity_test_index_a"));
                client.performRequest(new Request("DELETE", "zentity_test_index_b"));
                client.performRequest(new Request("DELETE", "zentity_test_index_c"));
                client.performRequest(new Request("DELETE", "zentity_test_index_d"));
                break;
        }
    }

    protected void destroyTestEntityModelA() throws IOException {
        client.performRequest(new Request("DELETE", "_zentity/models/zentity_test_entity_a"));
    }

    protected void destroyTestEntityModelB() throws IOException {
        client.performRequest(new Request("DELETE", "_zentity/models/zentity_test_entity_b"));
    }

    private void destroyTestEntityModelArrays() throws IOException {
        client.performRequest(new Request("DELETE", "_zentity/models/zentity_test_entity_arrays"));
    }

    private void destroyTestEntityModelElasticsearchError() throws IOException {
        client.performRequest(new Request("DELETE", "_zentity/models/zentity_test_entity_elasticsearch_error"));
    }

    private void destroyTestEntityModelZentityError() throws IOException {
        client.performRequest(new Request("DELETE", "_zentity/models/zentity_test_entity_zentity_error"));
    }

    protected void destroyTestResources(int testResourceSet) throws IOException {
        destroyTestIndices(testResourceSet);
        switch (testResourceSet) {
            case TEST_RESOURCES_A:
                destroyTestEntityModelA();
                break;
            case TEST_RESOURCES_B:
                destroyTestEntityModelB();
                break;
            case TEST_RESOURCES_ARRAYS:
                destroyTestEntityModelArrays();
                break;
            case TEST_RESOURCES_ELASTICSEARCH_ERROR:
                destroyTestEntityModelElasticsearchError();
                break;
            case TEST_RESOURCES_ZENTITY_ERROR:
                destroyTestEntityModelZentityError();
                break;
        }
    }

    protected byte[] getTestEntityModelAJson() throws IOException {
        return readFile("TestEntityModelA.json");
    }

    protected byte[] getTestEntityModelBJson() throws IOException {
        return readFile("TestEntityModelB.json");
    }

    protected void prepareTestEntityModelA() throws Exception {
        ByteArrayEntity testEntityModelA = new ByteArrayEntity(getTestEntityModelAJson(), ContentType.APPLICATION_JSON);
        Request postModelA = new Request("POST", "_zentity/models/zentity_test_entity_a");
        postModelA.setEntity(testEntityModelA);
        client.performRequest(postModelA);
    }

    protected void prepareTestEntityModelB() throws Exception {
        ByteArrayEntity testEntityModelB = new ByteArrayEntity(getTestEntityModelBJson(), ContentType.APPLICATION_JSON);
        Request postModelB = new Request("POST", "_zentity/models/zentity_test_entity_b");
        postModelB.setEntity(testEntityModelB);
        client.performRequest(postModelB);
    }

    private void prepareTestEntityModelArrays() throws Exception {
        ByteArrayEntity testEntityModelArrays = new ByteArrayEntity(readFile("TestEntityModelArrays.json"), ContentType.APPLICATION_JSON);
        Request postModelArrays = new Request("POST", "_zentity/models/zentity_test_entity_arrays");
        postModelArrays.setEntity(testEntityModelArrays);
        client.performRequest(postModelArrays);
    }

    private void prepareTestEntityModelElasticsearchError() throws Exception {
        ByteArrayEntity testEntityModelElasticsearchError = new ByteArrayEntity(readFile("TestEntityModelElasticsearchError.json"), ContentType.APPLICATION_JSON);
        Request postModelElasticsearchError = new Request("POST", "_zentity/models/zentity_test_entity_elasticsearch_error");
        postModelElasticsearchError.setEntity(testEntityModelElasticsearchError);
        client.performRequest(postModelElasticsearchError);
    }

    private void prepareTestEntityModelZentityError() throws Exception {
        ByteArrayEntity testEntityModelZentityError = new ByteArrayEntity(readFile("TestEntityModelZentityError.json"), ContentType.APPLICATION_JSON);
        Request postModelZentityError = new Request("POST", "_zentity/models/zentity_test_entity_zentity_error");
        postModelZentityError.setEntity(testEntityModelZentityError);
        client.performRequest(postModelZentityError);
    }

    protected void prepareTestIndices(int testResourceSet) throws Exception {

        // Load files
        ByteArrayEntity testIndex;
        ByteArrayEntity testData;

        // Elasticsearch 7.0.0+ removes mapping types

        Properties props = ZentityPlugin.properties();
        if (testResourceSet == TEST_RESOURCES_ARRAYS) {
            if (props.getProperty("elasticsearch.version").compareTo("7.") >= 0) {
                testIndex = new ByteArrayEntity(readFile("TestIndexArrays.json"), ContentType.APPLICATION_JSON);
                testData = new ByteArrayEntity(readFile("TestDataArrays.ndjson"), ContentType.create("application/x-ndjson"));
            } else {
                testIndex = new ByteArrayEntity(readFile("TestIndexArraysElasticsearch6.json"), ContentType.APPLICATION_JSON);
                testData = new ByteArrayEntity(readFile("TestDataArraysElasticsearch6.ndjson"), ContentType.create("application/x-ndjson"));
            }
            Request putTestIndexArrays = new Request("PUT", "zentity_test_index_arrays");
            putTestIndexArrays.setEntity(testIndex);
            client.performRequest(putTestIndexArrays);
        } else {
            if (props.getProperty("elasticsearch.version").compareTo("7.") >= 0) {
                testIndex = new ByteArrayEntity(readFile("TestIndex.json"), ContentType.APPLICATION_JSON);
                testData = new ByteArrayEntity(readFile("TestData.ndjson"), ContentType.create("application/x-ndjson"));
            } else {
                testIndex = new ByteArrayEntity(readFile("TestIndexElasticsearch6.json"), ContentType.APPLICATION_JSON);
                testData = new ByteArrayEntity(readFile("TestDataElasticsearch6.ndjson"), ContentType.create("application/x-ndjson"));
            }
            Request putTestIndexA = new Request("PUT", "zentity_test_index_a");
            putTestIndexA.setEntity(testIndex);
            client.performRequest(putTestIndexA);
            Request putTestIndexB = new Request("PUT", "zentity_test_index_b");
            putTestIndexB.setEntity(testIndex);
            client.performRequest(putTestIndexB);
            Request putTestIndexC = new Request("PUT", "zentity_test_index_c");
            putTestIndexC.setEntity(testIndex);
            client.performRequest(putTestIndexC);
            Request putTestIndexD = new Request("PUT", "zentity_test_index_d");
            putTestIndexD.setEntity(testIndex);
            client.performRequest(putTestIndexD);
        }


        // Load data into indices
        Request postBulk = new Request("POST", "_bulk");
        postBulk.addParameter("refresh", "true");
        postBulk.setEntity(testData);
        client.performRequest(postBulk);
    }

    protected void prepareTestResources(int testResourceSet) throws Exception {
        prepareTestIndices(testResourceSet);
        switch (testResourceSet) {
            case TEST_RESOURCES_A:
                prepareTestEntityModelA();
                break;
            case TEST_RESOURCES_B:
                prepareTestEntityModelB();
                break;
            case TEST_RESOURCES_ARRAYS:
                prepareTestEntityModelArrays();
                break;
            case TEST_RESOURCES_ELASTICSEARCH_ERROR:
                prepareTestEntityModelElasticsearchError();
                break;
            case TEST_RESOURCES_ZENTITY_ERROR:
                prepareTestEntityModelZentityError();
                break;
        }
    }

    /**
     * @param json The JSON response.
     * @return A CSV set of "id,hop number" strings.
     */
    protected Set<String> getActualIdHits(JsonNode json) {
        Set<String> docsActual = new TreeSet<>();
        for (JsonNode node : json.get("hits").get("hits")) {
            String id = node.get("_id").asText();
            int hop = node.get("_hop").asInt();
            docsActual.add(id + "," + hop);
        }
        return docsActual;
    }

}
