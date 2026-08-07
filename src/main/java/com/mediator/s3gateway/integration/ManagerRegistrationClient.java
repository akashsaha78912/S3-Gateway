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

    public HeadObjectResponse headObject(String objectName, String category) {
        String url = properties.getHead().getUrl()
                + "?objectName={objectName}&category={category}";

        return restClient.get()
                .uri(url, objectName, category)
                .retrieve()
                .body(HeadObjectResponse.class);
    }
    public void register(
            String operation,
            String category,
            String key,
            int contentLength
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

        restClient.post()
                .uri(manager.getRegisterUrl())
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .toBodilessEntity();
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
            @JsonProperty("am_objectSize")
            String objectSize,
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
            return Long.parseLong(objectSize);
        }
    }
}
