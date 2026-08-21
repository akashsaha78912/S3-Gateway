package com.mediator.s3gateway.config;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Type-safe view of the {@code gateway.*} settings in application.yml.
 *
 * <p>
 * Spring populates this object during application startup.
 */
@ConfigurationProperties("gateway")
public class GatewayProperties {

    private Manager manager = new Manager();
    private Head head = new Head();
    private Progress progress = new Progress();
    private UpdateComment updateComment= new UpdateComment();
    public Manager getManager() {
        return manager;
    }

    public void setManager(Manager manager) {
        this.manager = manager;
    }

    public Head getHead() {
        return head;
    }

    public void setHead(Head head) {
        this.head = head;
    }
    public Progress getProgress() {
        return progress;
    }
    public void setProgress(Progress progress) {
        this.progress = progress;
    }
    public void setComment(UpdateComment updateComment){
        this.updateComment=updateComment;
    }
    public UpdateComment getComment(){
        return updateComment;
    }

    /**
     * Root directory under which all gateway-owned NLD data is stored.
     */
    private Path nearlineRoot = Path.of("./data/nld");

    /**
     * Fixed mapping from public S3 bucket names to NLD category names.
     */
    private Map<String, String> buckets = new LinkedHashMap<>();

    /**
     * Settings for background request-registry work.
     */
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

        /**
         * Number of background worker threads.
         */
        private int workers = 2;

        public int getWorkers() {
            return workers;
        }

        public void setWorkers(int workers) {
            this.workers = workers;
        }
    }

    public static class Progress {

        private String url;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

    }
    public static class UpdateComment{
        public String url;

        public String getUrl(){
            return url;
        }
        public void setUrl(String url){
            this.url=url;
        }
    }

    public static class Head {

        private String url;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }
    }

    public static class Manager {

        private String registerUrl;
        private String sourceDestination = "Mover1";
        private String rootPath;
        private String qos = "0";
        private String moverName = "Mover1";
        private String diskName = "mover1_disk";
        private String options = "";
        private Map<String,String> comments = new HashMap<>();
        private String instanceNumber = "-1";
        private String media = "NLD_DISK";
        private int priority = 50;

        public String getRegisterUrl() {
            return registerUrl;
        }

        public void setRegisterUrl(String registerUrl) {
            this.registerUrl = registerUrl;
        }

        public String getSourceDestination() {
            return sourceDestination;
        }

        public void setSourceDestination(String sourceDestination) {
            this.sourceDestination = sourceDestination;
        }

        public String getRootPath() {
            return rootPath;
        }

        public void setRootPath(String rootPath) {
            this.rootPath = rootPath;
        }

        public String getQos() {
            return qos;
        }

        public String getMoverName() {
            return moverName;
        }

        public void setMoverName(String moverName) {
            this.moverName = moverName;
        }

        public String getDiskName() {
            return diskName;
        }

        public void setDiskName(String diskName) {
            this.diskName = diskName;
        }

        public void setQos(String qos) {
            this.qos = qos;
        }

        public String getOptions() {
            return options;
        }

        public void setOptions(String options) {
            this.options = options;
        }

        public Map<String,String> getComments() {
            return comments == null ? new HashMap<>() : comments;
        }

        public void setComments(Map<String,String> comments) {
            this.comments = comments == null ? new HashMap<>() : new HashMap<>(comments);
        }

        public String getInstanceNumber() {
            return instanceNumber;
        }

        public void setInstanceNumber(String instanceNumber) {
            this.instanceNumber = instanceNumber;
        }

        public String getMedia() {
            return media;
        }

        public void setMedia(String media) {
            this.media = media;
        }

        public int getPriority() {
            return priority;
        }

        public void setPriority(int priority) {
            this.priority = priority;
        }
    }

}
