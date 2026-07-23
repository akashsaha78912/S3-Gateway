package com.mediator.s3gateway.config;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Type-safe view of the {@code gateway.*} settings in application.yml.
 *
 * <p>Spring populates this object during application startup.
 */
@ConfigurationProperties("gateway")
public class GatewayProperties {

    /** Root directory under which all gateway-owned NLD data is stored. */
    private Path nearlineRoot = Path.of("./data/nld");

    /** Fixed mapping from public S3 bucket names to NLD category names. */
    private Map<String, String> buckets = new LinkedHashMap<>();

    /** Settings for background request-registry work. */
    private Async async = new Async();

    public Path getNearlineRoot() {
        return nearlineRoot;
    }

    public void setNearlineRoot(Path nearlineRoot) {
        this.nearlineRoot = nearlineRoot;
    }

    public Map<String, String> getBuckets() {
        return buckets;
    }

    public void setBuckets(Map<String, String> buckets) {
        this.buckets = buckets;
    }

    public Async getAsync() {
        return async;
    }

    public void setAsync(Async async) {
        this.async = async;
    }

    /**
     * Configuration for the RequestRegistry executor.
     */
    public static class Async {

        /** Number of background worker threads. */
        private int workers = 2;

        public int getWorkers() {
            return workers;
        }

        public void setWorkers(int workers) {
            this.workers = workers;
        }
    }
}
