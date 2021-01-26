package io.zentity.devtools;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.terma.javaniotcpproxy.StaticTcpProxyConfig;
import com.github.terma.javaniotcpproxy.TcpProxy;
import io.zentity.common.Json;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
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
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public abstract class AbstractITCase {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractITCase.class);

    private static final String DEFAULT_TAG = Optional
        .ofNullable(System.getenv("ELASTICSEARCH_VERSION"))
        .orElse("7.10.2");

    private static final DockerImageName DEFAULT_IMAGE = DockerImageName
        .parse("docker.elastic.co/elasticsearch/elasticsearch-oss")
        .withTag(DEFAULT_TAG);

    private static final String PLUGIN_DIR = Objects.requireNonNull(System.getenv("PLUGIN_BUILD_DIR"), "Must specify PLUGIN_BUILD_DIR");

    private static final Boolean DEBUGGER_ENABLED = Optional.ofNullable(System.getenv("DEBUGGER_ENABLED"))
        .map(Boolean::valueOf)
        .orElse(false);

    private static final Integer DEBUGGER_PORT = Optional.ofNullable(System.getenv("DEBUGGER_PORT"))
        .map(Integer::valueOf)
        .orElseGet(AbstractITCase::getRandomPort);

    private static final Duration DEBUGGER_SLEEP = Optional.ofNullable(System.getenv("DEBUGGER_SLEEP"))
        .map(Long::valueOf)
        .map(Duration::ofMillis)
        .orElseGet(() -> Duration.ofSeconds(5));

    /**
     * The max amount of socket inactivity in ms for the {@link RestClient ES RestClient}. Helpful to increase when
     * stepping through breakpoints during plugin debuggin'.
     */
    private static final Duration CLIENT_SOCKET_TIMEOUT = Optional.ofNullable(System.getenv("CLIENT_SOCKET_TIMEOUT"))
        .map(Long::valueOf)
        .map(Duration::ofMillis)
        .orElseGet(() -> DEBUGGER_ENABLED ? Duration.ofMinutes(5) : Duration.ofSeconds(30));

    protected static RestClient client;

    private static TcpProxy debuggerProxy;

    @ClassRule
    public static final PluggableElasticsearchContainer ES_CONTAINER = buildEsContainer();

    private static Integer getRandomPort() {
        Random rand = new Random();
        return rand.ints(20_000, 60_000)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No random port generated"));
    }

    private static PluggableElasticsearchContainer buildEsContainer() {
        PluggableElasticsearchContainer container = (PluggableElasticsearchContainer) new PluggableElasticsearchContainer(DEFAULT_IMAGE)
            .withPluginDir(Paths.get(PLUGIN_DIR))
            // enable assertions
            // see: https://github.com/zentity-io/zentity/issues/64
            .withEsJavaOpt("-ea")
            .withLogConsumer(
                new Slf4jLogConsumer(LOG)
                    .withSeparateOutputStreams()
                    .withPrefix("zentity-es")
            );

        if (DEBUGGER_ENABLED) {
            LOG.info("Starting remote ES debugger on port {}", DEBUGGER_PORT);
            container.withDebugger(DEBUGGER_PORT);
        }

        return container;
    }

    private static RestClient buildRestClient() {
        return RestClient
            .builder(HttpHost.create(ES_CONTAINER.getHttpHostAddress()))
            .setHttpClientConfigCallback(builder -> builder.setDefaultRequestConfig(
                RequestConfig
                    .custom()
                    .setSocketTimeout(
                        Long.valueOf(CLIENT_SOCKET_TIMEOUT.toMillis()).intValue()
                    )
                    .build()
            ))
            .build();
    }

    @BeforeClass
    public static void startDebuggerProxy() throws InterruptedException {
        if (!DEBUGGER_ENABLED) {
            return;
        }

        final StaticTcpProxyConfig config = new StaticTcpProxyConfig(
            DEBUGGER_PORT,
            ES_CONTAINER.getContainerIpAddress(),
            ES_CONTAINER.getMappedPort(DEBUGGER_PORT)
        );
        config.setWorkerCount(1);

        debuggerProxy = new TcpProxy(config);
        debuggerProxy.start();
        // Sleep so the debugger can be manually attached
        LOG.info("Sleeping {}ms to allow debugger attaching.", DEBUGGER_SLEEP.toMillis());
        Thread.sleep(DEBUGGER_SLEEP.toMillis());
    }

    @BeforeClass
    public static void startRestClient() throws IOException {
        client = buildRestClient();
        try {
            Response response = client.performRequest(new Request("GET", "/"));
            JsonNode json = Json.MAPPER.readTree(response.getEntity().getContent());
            assertEquals("You Know, for Search", json.get("tagline").textValue());
        } catch (IOException e) {
            stopRestClient();
            // If we have an exception here, let's ignore the test
            assumeFalse("Integration tests are skipped", e.getMessage().contains("Connection refused"));
            fail("Something wrong is happening. REST Client seemed to raise an exception: " + e.getMessage());
        }
    }

    @AfterClass
    public static void stopRestClient() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @AfterClass
    public static void stopDebugger() {
        if (debuggerProxy != null) {
            debuggerProxy.shutdown();
        }
    }
}
