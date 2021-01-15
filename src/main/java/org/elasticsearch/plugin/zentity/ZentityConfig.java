package org.elasticsearch.plugin.zentity;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class ZentityConfig {

    private static final Setting<Integer> RESOLUTION_MAX_CONCURRENT_JOBS = Setting
        .intSetting("resolution.max_concurrent_jobs", 10_000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private static final Setting<Integer> RESOLUTION_MAX_CONCURRENT_JOBS_PER_REQUEST = Setting
        .intSetting("resolution.max_concurrent_jobs_per_request", 100, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private static final Setting<String> MODELS_INDEX_NAME = Setting
        .simpleString("index.name", ".zentity-models", Setting.Property.NodeScope, Setting.Property.Final);

    private static final Setting<Integer> MODELS_INDEX_DEFAULT_NUMBER_OF_SHARDS = Setting
        .intSetting("index.default_number_of_shards", 1, Setting.Property.NodeScope, Setting.Property.Final);

    private static final Setting<Integer> MODELS_INDEX_DEFAULT_NUMBER_OF_REPLICAS = Setting
        .intSetting("index.default_number_of_replicas", 1, Setting.Property.NodeScope, Setting.Property.Final);

    private final Settings settings;

    public ZentityConfig(Environment env) {
        // Elasticsearch config directory
        final Path configDir = env.configFile();

        // Resolve the plugin's custom settings file
        final Path settingsYamlFile = configDir.resolve("zentity.yml");

        final Settings.Builder settingsBuilder = Settings.builder();
        if (settingsYamlFile.toFile().exists()) {
            try {
                settingsBuilder.loadFromPath(settingsYamlFile);
            } catch (IOException ex) {
                throw new ElasticsearchException("Failed to load settings", ex);
            }
        }

        settings = settingsBuilder.build();
    }

    public int getResolutionMaxConcurrentJobsPerRequest() {
        return RESOLUTION_MAX_CONCURRENT_JOBS_PER_REQUEST.get(settings);
    }

    public int getResolutionMaxConcurrentJobs() {
        return RESOLUTION_MAX_CONCURRENT_JOBS.get(settings);
    }

    public String getModelsIndexName() {
        return MODELS_INDEX_NAME.get(settings);
    }

    public int getModelsIndexDefaultNumberOfShards() {
        return MODELS_INDEX_DEFAULT_NUMBER_OF_SHARDS.get(settings);
    }

    public int getModelsIndexDefaultNumberOfReplicas() {
        return MODELS_INDEX_DEFAULT_NUMBER_OF_REPLICAS.get(settings);
    }

    public List<Setting<?>> getSettings() {
        return List.of(
            RESOLUTION_MAX_CONCURRENT_JOBS,
            RESOLUTION_MAX_CONCURRENT_JOBS_PER_REQUEST,
            MODELS_INDEX_NAME,
            MODELS_INDEX_DEFAULT_NUMBER_OF_SHARDS,
            MODELS_INDEX_DEFAULT_NUMBER_OF_REPLICAS
        );
    }
}
