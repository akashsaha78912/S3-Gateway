package com.mediator.s3gateway.config;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("gateway")
public class GatewayProperties {

       private Path nearlineRoot = Path.of("./data/nld");
    private Map<String, String> buckets = new LinkedHashMap<>();
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

    public static class Async {

        private int workers = 2;

        public int getWorkers() {
            return workers;
        }

        public void setWorkers(int workers) {
            this.workers = workers;
        }
    }
}
