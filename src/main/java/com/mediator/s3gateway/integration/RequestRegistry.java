package com.mediator.s3gateway.integration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.mediator.s3gateway.config.GatewayProperties;
import com.mediator.s3gateway.storage.NearlineStore;

/**
 * File-backed boundary between this standalone gateway and future Mediator
 * processing.
 *
 * <p>
 * Each archive or restore request is written below {@code .gateway-requests}.
 * The object itself remains in the NLD store. Replace the marked executor body
 * with the real Mediator Manager API when that integration phase begins.
 */
@Service
public class RequestRegistry {

    private final GatewayProperties properties;
    private final NearlineStore store;
    private final ExecutorService executor;
    private final ManagerRegistrationClient managerClient;

    /**
     * Creates the registry and its configured fixed-size background executor.
     */
    public RequestRegistry(GatewayProperties properties, NearlineStore store, ManagerRegistrationClient managerClient) {
        this.properties = properties;
        this.store = store;
        this.managerClient = managerClient;
        this.executor = Executors.newFixedThreadPool(properties.getAsync().getWorkers());
    }
    private static final Logger log
            = LoggerFactory.getLogger(RequestRegistry.class);

    /**
     * Persists a new request, then advances its status asynchronously.
     *
     * @return the UUID assigned to the request record
     */
    public int submit(String operation, String bucket, String key, long contentLength, Map<String,String> userMetadata,Map<String, String> tags) {
        String category = store.category(bucket);
        // String id = UUID.randomUUID().toString();
        // Path requestFile = properties.getNearlineRoot().toAbsolutePath().resolve(".gateway-requests").resolve(id + ".properties");
        // write(requestFile, operation, category, key, "ACCEPTED");
        // This background transition is the placeholder for a Manager call.
        // executor.submit(() -> {
        //     write(requestFile, operation, category, key, "SUBMITTED_TO_MANAGER");
        //     /* MediatorManager.submit(request) belongs here. */ });

        // executor.submit(() -> {
        //     try {
        //          managerClient.register(operation, category, key, contentLength);
        //         write(
        //                 requestFile,
        //                 operation,
        //                 category,
        //                 key,
        //                 "SUBMITTED_TO_MANAGER"
        //         );
        //     } catch (Exception e) {
        //         log.error(
        //                 "Manager registration failed: requestId={}, operation={}, category={}, key={}",
        //                 id,
        //                 operation,
        //                 category,
        //                 key,
        //                 e
        //         );
        //         write(
        //                 requestFile,
        //                 operation,
        //                 category,
        //                 key,
        //                 "SUBMISSION_FAILED"
        //         );
        //     }
        // });
        ManagerRegistrationClient.RegisterResponse response
                = managerClient.register(operation, category, key, contentLength, userMetadata,tags);
        if (response == null) {
            throw new IllegalStateException(
                    "Manager registration returned an empty response"
            );
        }

        log.info(
                "Manager registration response: operation={}, category={}, key={}, reqID={}, status={}",
                operation,
                category,
                key,
                response.reqID(),
                response.status()
        );

        // write(
        //         requestFile,
        //         operation,
        //         category,
        //         key,
        //         "SUBMITTED_TO_MANAGER"
        // );
        return response.reqID();

    }

    /**
     * Finds the first recorded RESTORE request for the resolved object.
     */
    public Optional<String> activeRestore(String bucket, String key) {
        String category = store.category(bucket);
        Path dir = properties.getNearlineRoot().toAbsolutePath().resolve(".gateway-requests");
        if (!Files.isDirectory(dir)) {
            return Optional.empty();
        }
        try (var paths = Files.list(dir)) {
            return paths.filter(p -> p.toString().endsWith(".properties")).map(p -> {
                try {
                    return Files.readString(p);
                } catch (IOException e) {
                    return "";
                }
            }).filter(v -> v.contains("operation=RESTORE\n") && v.contains("category=" + category + "\n") && v.contains("key=" + key + "\n")).map(v -> value(v, "id")).findFirst();
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    /**
     * Rewrites one request record with its latest status and timestamp.
     */
    private void write(Path file, String operation, String category, String key, String status) {
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, "id=" + file.getFileName().toString().replace(".properties", "") + "\noperation=" + operation + "\ncategory=" + category + "\nkey=" + key + "\nstatus=" + status + "\ncreated=" + Instant.now() + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot persist gateway request", e);
        }
    }

    /**
     * Extracts one {@code name=value} field from a request-record string.
     */
    private String value(String s, String field) {
        for (String line : s.split("\\R")) {
            if (line.startsWith(field + "=")) {
                return line.substring(field.length() + 1);

            }
        }
        return "";
    }
}
