package io.zentity.resolution;

import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An {@link ElasticsearchContainer} that allows installing plugins, either official ones or from a local host directory.
 */
public class PluggableElasticsearchContainer extends ElasticsearchContainer {
    private final String PLUGINS_CONTAINER_PATH = "/plugins/";
    private final List<String> plugins = new ArrayList<>();

    public PluggableElasticsearchContainer(DockerImageName imageName) {
        super(imageName);
    }

    /**
     * Install a plugin on startup.
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/plugins/current/installation.html"></a>
     * @param pluginNameOrUrl Name or URL of the plugin.
     * @return The container.
     */
    public PluggableElasticsearchContainer withPlugin(String pluginNameOrUrl) {
        plugins.add(pluginNameOrUrl);
        return this;
    }

    /**
     *
     * @param pluginHostPath The path to the plugin zip file on the host machine.
     * @return The container.
     */
    public PluggableElasticsearchContainer withLocalPlugin(Path pluginHostPath) {
        Path plugin = pluginHostPath.getFileName();
        withCopyFileToContainer(
            MountableFile.forHostPath(pluginHostPath),
            PLUGINS_CONTAINER_PATH + plugin
        );
        withPlugin("file://" + PLUGINS_CONTAINER_PATH + plugin);
        return this;
    }

    /**
     * @param pluginDir The plugins directory.
     * @return The container.
     * @see <a href="https://github.com/dadoonet/testcontainers-java-module-elasticsearch/blob/780eec66c2999a1e4814f039b2a4559d6a5da408/src/main/java/fr/pilato/elasticsearch/containers/ElasticsearchContainer.java#L113-L142"></a>
     * @see <a href="https://github.com/testcontainers/testcontainers-java/issues/1921"></a>
     */
    public PluggableElasticsearchContainer withPluginDir(Path pluginDir) {
        Objects.requireNonNull(pluginDir, "Must define plugin directory");

        logger().debug("Installing plugins from [{}]", pluginDir);
        try {
            Files.list(pluginDir).forEach(path -> {
                logger().trace("File found in [{}]: [{}]", pluginDir, path);
                if (path.toString().endsWith(".zip")) {
                    withLocalPlugin(path);
                }
            });
        } catch (IOException e) {
            logger().error("Error listing local plugins", e);
        }
        return this;
    }

    @Override
    protected void configure() {
        super.configure();
        if (!plugins.isEmpty()) {
            String installPluginCmd = "elasticsearch-plugin install "
                + String.join(" ", plugins)
                + " && elasticsearch";
            String[] cmdParts = {
                "/bin/bash",
                "-c",
                installPluginCmd
            };

            withCommand(cmdParts);

            // ensure the container runs as elasticsearch as the server cannot be started as root,
            // which is what the entrypoint runs as
            withCreateContainerCmdModifier(cmd -> cmd.withUser("elasticsearch"));
        }
    }
}
