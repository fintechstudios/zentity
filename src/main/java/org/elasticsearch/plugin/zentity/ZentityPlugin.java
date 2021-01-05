package org.elasticsearch.plugin.zentity;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

public class ZentityPlugin extends Plugin implements ActionPlugin {

    private static final Properties PROPERTIES = new Properties();

    private static Properties loadPropertiesFromResources(String resourcePath) throws IOException {
        Properties props = new Properties();
        InputStream inputStream = ZentityPlugin.class.getResourceAsStream(resourcePath);
        props.load(inputStream);
        return props;
    }

    static {
        try {
            Properties zentityProperties = loadPropertiesFromResources("/zentity.properties");
            Properties pluginDescriptorProperties = loadPropertiesFromResources("/plugin-descriptor.properties");
            PROPERTIES.putAll(zentityProperties);
            PROPERTIES.putAll(pluginDescriptorProperties);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
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
}
