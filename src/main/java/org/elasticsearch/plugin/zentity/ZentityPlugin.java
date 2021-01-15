package org.elasticsearch.plugin.zentity;

import io.zentity.common.FunctionalUtil.UnCheckedConsumer;
import io.zentity.common.Json;
import io.zentity.common.SecurityUtil;
import io.zentity.resolution.BulkResolutionResponse;
import io.zentity.resolution.FilterTree;
import io.zentity.resolution.LoggedFilter;
import io.zentity.resolution.LoggedQuery;
import io.zentity.resolution.LoggedSearch;
import io.zentity.resolution.ResolutionResponse;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Supplier;

public class ZentityPlugin extends Plugin implements SystemIndexPlugin {

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
        SecurityUtil.doPrivileged((CheckedSupplier<?, ?>) () -> {
            // kick jackson to do some static caching of declared members info
            // Maybe move this to another place, but it is very hacky so maybe ok to leave here?
            Json.MAPPER.readTree("{}");

            // If a class is to be written directly (i.e. not using a type-specific StdSerializer instance),
            // it must be serialized here to initialize the jackson reflection cache

            LoggedFilter filter = new LoggedFilter();
            Map<String, Collection<String>> resolverAttrs = new TreeMap<>();
            resolverAttrs.put("init", Collections.singleton("attr"));
            filter.resolverAttributes = resolverAttrs;
            Map<Integer, FilterTree> groupedTree = new TreeMap<>();
            FilterTree filterTree = new FilterTree();
            filterTree.put("init", new FilterTree());
            groupedTree.put(0, filterTree);
            filter.groupedTree = groupedTree;

            LoggedSearch search = new LoggedSearch();
            SearchRequestBuilder searchRequest = new SearchRequestBuilder(null, SearchAction.INSTANCE);
            searchRequest.setQuery(QueryBuilders.matchAllQuery());
            search.searchRequest = searchRequest;
            SearchResponseSections sections = new SearchResponseSections(
                SearchHits.empty(true),
                new Aggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                false,
                null,
                new SearchProfileShardResults(new TreeMap<>()),
                1
            );
            search.response = new SearchResponse(
                sections,
                "some-scroll-id",
                3,
                2,
                1,
                300,
                new ShardSearchFailure[]{},
                SearchResponse.Clusters.EMPTY
            );
            search.responseError = new ElasticsearchStatusException("This was not found", RestStatus.NOT_FOUND);

            LoggedQuery query = new LoggedQuery();
            Map<String, LoggedFilter> filters = new TreeMap<>();
            filters.put("init", filter);
            query.filters = filters;
            query.hop = 1;
            query.index = "init-idx";
            query.search = search;

            ResolutionResponse res = new ResolutionResponse();
            res.error = null;
            res.hits = List.of();
            res.includeHits = true;
            res.includeQueries = true;
            res.includeStackTrace = true;
            res.took = Duration.ZERO;
            res.queries = List.of(query);

            BulkResolutionResponse bulkRes = new BulkResolutionResponse();
            bulkRes.tookMs = 100;
            bulkRes.errors = false;
            bulkRes.items = List.of(res);

            List<Object> objectsToSerialize = List.of(
                filter,
                search,
                query,
                res,
                bulkRes
            );

            objectsToSerialize.forEach(UnCheckedConsumer.from((obj) -> {
                Json.MAPPER.writeValueAsString(obj);
                Json.ORDERED_MAPPER.writeValueAsString(obj);
            }));
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
        return List.of(
            new HomeAction(config),
            new ModelsAction(config),
            new ResolutionAction(config),
            new SetupAction(config)
        );
    }

    /**
     * Returns a list of additional {@link Setting} definitions for this plugin.
     */
    @Override
    public List<Setting<?>> getSettings() {
        return config.getSettings();
    }

    /**
     * Returns a {@link Collection} of {@link SystemIndexDescriptor}s that describe this plugin's system indices, including
     * name, mapping, and settings.
     *
     * @param settings The node's settings.
     * @return Descriptions of the system indices managed by this plugin.
     */
    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            new SystemIndexDescriptor(config.getModelsIndexName(), "The main index for storing Zentity Models")
        );
    }
}
