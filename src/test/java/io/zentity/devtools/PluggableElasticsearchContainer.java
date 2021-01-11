package io.zentity.devtools;

import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * An {@link ElasticsearchContainer} that allows installing plugins, either official ones or from a local host directory.
 */
public class PluggableElasticsearchContainer extends ElasticsearchContainer {
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
     * Load a plugin from a host path with a custom predicate for filtering files.
     *
     * @param pluginHostPath The path to the plugin zip file on the host machine.
     * @return The container.
     */
    public PluggableElasticsearchContainer withLocalPlugin(Path pluginHostPath) {
        Path plugin = pluginHostPath.getFileName();
        String pluginsContainerPath = "/plugins/";

        withCopyFileToContainer(
            MountableFile.forHostPath(pluginHostPath),
            pluginsContainerPath + plugin
        );
        withPlugin("file://" + pluginsContainerPath + plugin);
        return this;
    }

    /**
     * Load all the plugins from a directory on the host with a custom predicate for filtering files.
     *
     * @see <a href="https://github.com/dadoonet/testcontainers-java-module-elasticsearch/blob/780eec66c2999a1e4814f039b2a4559d6a5da408/src/main/java/fr/pilato/elasticsearch/containers/ElasticsearchContainer.java#L113-L142"></a>
     * @see <a href="https://github.com/testcontainers/testcontainers-java/issues/1921"></a>
     * @param pluginDir The plugins directory.
     * @param pluginFilePredicate The predicate to determine which files to include.
     * @return The container.
     */
    public PluggableElasticsearchContainer withPluginDir(Path pluginDir, Predicate<Path> pluginFilePredicate) {
        Objects.requireNonNull(pluginDir, "Must define plugin directory");
        Objects.requireNonNull(pluginFilePredicate, "Must define plugin directory");

        logger().debug("Installing plugins from [{}]", pluginDir);
        try {
            Files.list(pluginDir)
                .filter(pluginFilePredicate)
                .forEach(path -> {
                    logger().trace("Loading plugin found in [{}]: [{}]", pluginDir, path);
                    withLocalPlugin(path);
            });
        } catch (IOException e) {
            logger().error("Error listing local plugins", e);
        }
        return this;
    }

    /**
     * Load all the ZIP plugins from a directory on the host.
     *
     * @see <a href="https://github.com/dadoonet/testcontainers-java-module-elasticsearch/blob/780eec66c2999a1e4814f039b2a4559d6a5da408/src/main/java/fr/pilato/elasticsearch/containers/ElasticsearchContainer.java#L113-L142"></a>
     * @see <a href="https://github.com/testcontainers/testcontainers-java/issues/1921"></a>
     * @param pluginDir The plugins directory.
     * @return The container.
     */
    public PluggableElasticsearchContainer withPluginDir(Path pluginDir) {
        return withPluginDir(pluginDir, (path) -> path.toString().endsWith(".zip"));
    }

    public PluggableElasticsearchContainer withEsJavaOpt(String opt) {
        String currentJavaOpts = getEnvMap().getOrDefault("ES_JAVA_OPTS", "");
        String javaOpts = currentJavaOpts + " " + opt;
        withEnv("ES_JAVA_OPTS", javaOpts);
        return this;
    }

    /**
     * Expose a java debugger.
     *
     * @see <a href="https://bsideup.github.io/posts/debugging_containers/"></a>
     * @param port The port to run the debugger on.
     * @return The container.
     */
    public PluggableElasticsearchContainer withDebugger(int port) {
        addExposedPort(port);
        withEsJavaOpt("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:" + port);
        return this;
    }

    @Override
    protected void configure() {
        super.configure();
        if (!plugins.isEmpty()) {
            String installPluginCmd = "elasticsearch-plugin install --batch "
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
