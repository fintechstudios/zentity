package org.elasticsearch.plugin.zentity;

import io.zentity.common.Json;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

public class ZentityPlugin extends Plugin implements ActionPlugin {

    private static final Properties PROPERTIES = new Properties();

    public ZentityConfig config;

    static {
        try {
            Properties zentityProperties = loadPropertiesFromResources("/zentity.properties");
            Properties pluginDescriptorProperties = loadPropertiesFromResources("/plugin-descriptor.properties");
            PROPERTIES.putAll(zentityProperties);
            PROPERTIES.putAll(pluginDescriptorProperties);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        // Initializing Jackson requires reflection permissions
        // see: https://github.com/elastic/elasticsearch/blob/fc5725597189a4ee36b265a8fb75fa616b63e41b/plugins/discovery-ec2/src/main/java/org/elasticsearch/discovery/ec2/Ec2DiscoveryPlugin.java#L60-L74
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                // kick jackson to do some static caching of declared members info
                Json.MAPPER.readTree("{}");
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    public ZentityPlugin(final Settings settings, final Path configPath) {
        this.config = new ZentityConfig(new Environment(settings, configPath));
    }

    private static Properties loadPropertiesFromResources(String resourcePath) throws IOException {
        Properties props = new Properties();
        InputStream inputStream = ZentityPlugin.class.getResourceAsStream(resourcePath);
        props.load(inputStream);
        return props;
    }

    public static Properties properties() {
        return PROPERTIES;
    }

    public String version() {
        return PROPERTIES.getProperty("version");
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
            new HomeAction(),
            new ModelsAction(),
            new ResolutionAction(),
            new SetupAction()
        );
    }

    /**
     * Returns a list of additional {@link Setting} definitions for this plugin.
     */
    @Override
    public List<Setting<?>> getSettings() {
        return config.getSettings();
    }
}
