package com.mediator.s3gateway.integration;

import com.mediator.s3gateway.config.GatewayProperties;
import com.mediator.s3gateway.storage.NearlineStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.stereotype.Service;

/**
 * Temporary file-backed seam. Replace submit() with the Mediator Manager
 * request API.
 */
@Service
public class RequestRegistry {

    private final GatewayProperties properties;
    private final NearlineStore store;
    private final ExecutorService executor;

    public RequestRegistry(GatewayProperties properties, NearlineStore store) {
        this.properties = properties;
        this.store = store;
        this.executor = Executors.newFixedThreadPool(properties.getAsync().getWorkers());
    }

    public String submit(String operation, String bucket, String key) {
        String category = store.category(bucket);
        String id = UUID.randomUUID().toString();
        Path requestFile = properties.getNearlineRoot().toAbsolutePath().resolve(".gateway-requests").resolve(id + ".properties");
        write(requestFile, operation, category, key, "ACCEPTED");
        executor.submit(() -> {
            write(requestFile, operation, category, key, "SUBMITTED_TO_MANAGER");
            /* MediatorManager.submit(request) belongs here. */ });
        return id;
    }

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

    private void write(Path file, String operation, String category, String key, String status) {
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, "id=" + file.getFileName().toString().replace(".properties", "") + "\noperation=" + operation + "\ncategory=" + category + "\nkey=" + key + "\nstatus=" + status + "\ncreated=" + Instant.now() + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot persist gateway request", e);
        }
    }

    private String value(String s, String field) {
        for (String line : s.split("\\R")) {
            if (line.startsWith(field + "=")) {
                return line.substring(field.length() + 1);
        
            }
        }return "";
    }
}
