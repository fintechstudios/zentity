package io.zentity.resolution;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public abstract class AbstractITCase {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractITCase.class);

    private static final String DEFAULT_TAG = Optional
        .ofNullable(System.getenv("ELASTICSEARCH_VERSION"))
        .orElse("latest");

    private static final DockerImageName DEFAULT_IMAGE = DockerImageName
        .parse("docker.elastic.co/elasticsearch/elasticsearch-oss")
        .withTag(DEFAULT_TAG);

    private static final String PLUGIN_DIR = Objects.requireNonNull(System.getenv("PLUGIN_BUILD_DIR"), "Must specify PLUGIN_BUILD_DIR");

    protected static RestClient client;

    @ClassRule
    public static final PluggableElasticsearchContainer ES_CONTAINER = (PluggableElasticsearchContainer) new PluggableElasticsearchContainer(DEFAULT_IMAGE)
        .withPluginDir(Paths.get(PLUGIN_DIR))
        .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startRestClient() throws IOException {
        client = RestClient.builder(HttpHost.create(ES_CONTAINER.getHttpHostAddress())).build();
        try {
            Response response = client.performRequest(new Request("GET", "/"));
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
            assertEquals("You Know, for Search", json.get("tagline").textValue());
        } catch (IOException e) {
            // If we have an exception here, let's ignore the test
            assumeFalse("Integration tests are skipped", e.getMessage().contains("Connection refused"));
            fail("Something wrong is happening. REST Client seemed to raise an exception: " + e.getMessage());
            stopRestClient();
        }
    }

    @AfterClass
    public static void stopRestClient() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
