package com.mediator.s3gateway.integration;

import java.util.ArrayList;
import java.util.List;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.mediator.s3gateway.config.GatewayProperties;

@Service
public class ManagerRegistrationClient {

    private final RestClient restClient;
    private final GatewayProperties properties;

    public ManagerRegistrationClient(
            RestClient.Builder restClientBuilder,
            GatewayProperties properties
    ) {
        this.restClient = restClientBuilder.build();
        this.properties = properties;
    }

    public void setProgress(int reqID, int state, int progress, String remarks, String checksum) {

        RegisterProgress body = new RegisterProgress(
                reqID, state, progress, "Mover1", remarks, checksum);

        restClient.post()
                .uri(properties.getProgress().getUrl())
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .toBodilessEntity();
    }

    public HeadObjectResponse headObject(String objectName, String category) {
        String url = properties.getHead().getUrl()
                + "?objectName={objectName}&category={category}";

        return restClient.get()
                .uri(url, objectName, category)
                .retrieve()
                .body(HeadObjectResponse.class);
    }

    public RegisterResponse register(
            String operation,
            String category,
            String key,
            long contentLength
    ) {
        GatewayProperties.Manager manager = properties.getManager();

        String objectName = key.contains("/")
                ? key.substring(key.lastIndexOf('/') + 1)
                : key;
        List<FileItem> filelist = new ArrayList<>();
        filelist.add(new FileItem(objectName, contentLength));

        RegisterRequest body = new RegisterRequest(
                objectName,
                category,
                operation,
                manager.getSourceDestination(),
                manager.getRootPath(),
                manager.getQos(),
                manager.getMoverName(),
                manager.getDiskName(),
                manager.getOptions(),
                manager.getComments(),
                manager.getInstanceNumber(),
                filelist,
                manager.getMedia(),
                manager.getPriority()
        );

        return restClient.post()
                .uri(manager.getRegisterUrl())
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .body(RegisterResponse.class);
    }

    public record FileItem(
            String name,
            long size
            ) {

    }

    private record RegisterRequest(
            String objectName,
            String category,
            String requestType,
            String sourcedestination,
            String rootPath,
            String qos,
            String moverName,
            String DiskName,
            String options,
            String comments,
            String instancenumber,
            List<FileItem> filelist,
            String media,
            int priority
            ) {

    }

    public record HeadObjectResponse(
            @JsonProperty("am_object_name")
            String objectName,
            @JsonProperty("objectSizeBytes")
            String objectSizeBytes,
            @JsonProperty("am_ObjectChecksum")
            String checksum,
            @JsonProperty("am_archiveDate")
            String archiveDate,
            @JsonProperty("am_object_category")
            String category,
            @JsonProperty("status")
            int status
            ) {

        public long contentLength() {
            return Long.parseLong(objectSizeBytes);
        }
    }

    public record RegisterResponse(
            @JsonProperty("reqID")
            int reqID,
            @JsonProperty("status")
            int status
            ) {

    }

    public record RegisterProgress(
            @JsonProperty("reqID")
            int reqID,
            @JsonProperty("state")
            int state,
            @JsonProperty("progress")
            int progress,
            @JsonProperty("moverName")
            String moverName,
            @JsonProperty("remarks")
            String remarks,
            @JsonProperty("checksum")
            String checksum
            ) {

    }
}
