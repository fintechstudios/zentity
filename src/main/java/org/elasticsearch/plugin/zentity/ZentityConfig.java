package org.elasticsearch.plugin.zentity;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class ZentityConfig {

    private static final Setting<Integer> RESOLUTION_MAX_CONCURRENT_JOBS = Setting
        .intSetting("resolution.max_concurrent_jobs", 1000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private static final Setting<Integer> RESOLUTION_MAX_CONCURRENT_JOBS_PER_REQUEST = Setting
        .intSetting("resolution.max_concurrent_jobs_per_request", 100, Setting.Property.NodeScope, Setting.Property.Dynamic);

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

    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            RESOLUTION_MAX_CONCURRENT_JOBS,
            RESOLUTION_MAX_CONCURRENT_JOBS_PER_REQUEST
        );
    }
}
