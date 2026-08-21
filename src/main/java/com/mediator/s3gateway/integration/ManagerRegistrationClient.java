package com.mediator.s3gateway.integration;

//import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mediator.s3gateway.config.GatewayProperties;
import com.mediator.s3gateway.exception.S3Exception;

@Service
public class ManagerRegistrationClient {

    private final RestClient restClient;
    private final GatewayProperties properties;
    private final ObjectMapper objectMapper;

    public ManagerRegistrationClient(
            RestClient.Builder restClientBuilder,
            GatewayProperties properties,
            ObjectMapper objectMapper
    ) {
        this.restClient = restClientBuilder.build();
        this.properties = properties;
        this.objectMapper = objectMapper;
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

    public void setComment(String Objectname, String Category, String comments){
                String url=properties.getComment().getUrl();
                UpdateComment body= new UpdateComment(Objectname, Category, comments);

                restClient.post()
                .uri(url)
                .header(             HttpHeaders.AUTHORIZATION,
                    "Bearer eI0O1WW7Mi7Ik6OAMCYCxfO8E16Li-5f"
            )
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .toBodilessEntity();


    }

    public HeadObjectResponse headObject(String objectName, String category) {
        String normalizedKey = objectName.startsWith("/")
                ? objectName.substring(1)
                : objectName;

        int lastSlash = normalizedKey.lastIndexOf('/');
        if (lastSlash >= 0) {
            objectName = normalizedKey.substring(0, lastSlash);
        } else {
            objectName = normalizedKey;
        }

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
            long contentLength,
            Map<String, String> userMetadata,
            Map<String, String> tags
    ) {
        GatewayProperties.Manager manager = properties.getManager();
        String normalizedKey = key.startsWith("/")
                ? key.substring(1)
                : key;

        int lastSlash = normalizedKey.lastIndexOf('/');

        String objectName;
        String fileName;

        if (lastSlash >= 0) {
            objectName = normalizedKey.substring(0, lastSlash);
            fileName = normalizedKey.substring(lastSlash + 1);
        } else {
            // Root-level S3 object: no separate folder is available.
            objectName = normalizedKey;
            fileName = normalizedKey;
        }

        List<FileItem> filelist = new ArrayList<>();
        filelist.add(new FileItem(fileName, contentLength));
        String comments;
        try {
            comments = objectMapper.writeValueAsString(new S3Attributes(1, userMetadata, tags));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "Unable to serialize S3 metadata and tags",
                    e
            );
        }
        int commentSize=comments.getBytes(StandardCharsets.UTF_8).length;
        if (commentSize > 4096) {
            throw new S3Exception(400, "MetadataTooLarge", "combined metadata and tags exceed the manager comments limit", key);

        }

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
                //  manager.getComments(),
                comments,
                manager.getInstanceNumber(),
                filelist,
                manager.getMedia(),
                manager.getPriority()
        );
        System.out.println("RegisterRequest: " + body);
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
            @JsonProperty("comments")
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
            int status,
            @JsonProperty("am_objectComments")
            String comments,
            @JsonProperty("instances")
            List<InstanceResponse> instances
            ) {

        public long contentLength() {
            return Long.parseLong(objectSizeBytes);
        }
    }
    public record InstanceResponse(
        @JsonProperty("media")
        String media
    ){}

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

    public record S3Attributes(
            int version,
            Map<String, String> userMetadata,
            Map<String, String> tags
            ) {

        public S3Attributes {
            userMetadata = userMetadata == null ? Map.of() : Map.copyOf(userMetadata);
            tags = tags == null ? Map.of() : Map.copyOf(tags);
        }
    }
    public record UpdateComment(
        @JsonProperty("objectName")
        String objectName,
        @JsonProperty("category")
        String category,
        @JsonProperty("comments")
        String comments
    ){}
}
