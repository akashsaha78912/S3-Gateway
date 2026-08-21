package com.mediator.s3gateway.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.mediator.s3gateway.config.GatewayProperties;
import com.mediator.s3gateway.exception.S3Exception;

/**
 * Persists object metadata separately from object bytes.
 *
 * <p>
 * Metadata files live below {@code .gateway-metadata}. Category and object key
 * components are Base64 URL encoded so metadata paths remain safe and aliases
 * that resolve to the same category share the same metadata.
 */
@Service
public class ObjectMetadataStore {

    private static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    private final GatewayProperties properties;
    private final NearlineStore store;

    /**
     * Creates the metadata store using the configured NLD root.
     */
    public ObjectMetadataStore(GatewayProperties properties, NearlineStore store) {
        this.properties = properties;
        this.store = store;
    }

    /**
     * Loads metadata, applying S3-compatible defaults when no property exists.
     */
    public Metadata get(String bucket, String key) {
        Properties p = new Properties();
        Path path = file(bucket, key);
        if (Files.exists(path)) try (InputStream in = Files.newInputStream(path)) {
            p.load(in);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        Map<String, String> userMetadata = p.stringPropertyNames().stream()
                .filter(name -> name.startsWith("userMetadata."))
                .collect(Collectors.toUnmodifiableMap(name -> decode(name.substring("userMetadata.".length())), p::getProperty));
        ObjectHeaders headers = new ObjectHeaders(
                p.getProperty("contentType", DEFAULT_CONTENT_TYPE),
                p.getProperty("cacheControl"),
                p.getProperty("contentDisposition"),
                p.getProperty("contentEncoding"),
                p.getProperty("contentLanguage"),
                p.getProperty("expires"),
                userMetadata);
        long contentLength = parseContentLength(p.getProperty("contentLength"));
        String etag = p.getProperty("etag");
        Instant lastModified = parseInstant(p.getProperty("lastModified"));

        return new Metadata(p.getProperty("storageClass", "STANDARD"),
                p.getProperty("restoreExpiry"),
                contentLength, etag, lastModified,
                headers);
    }

    /**
     * Saves storage class, representation headers and x-amz-meta-* values.
     */
    public void put(String bucket, String key, String storageClass, ObjectHeaders headers, long contentLength, String etag, FileTime lastModified) {
        Properties p = new Properties();
        p.setProperty("storageClass", storageClass);
        p.setProperty("contentType", headers.contentType() == null || headers.contentType().isBlank() ? DEFAULT_CONTENT_TYPE : headers.contentType());
        p.setProperty("contentLength", Long.toString(contentLength));
        p.setProperty("etag", etag);
        p.setProperty("lastModified", lastModified.toInstant().toString());
        set(p, "cacheControl", headers.cacheControl());
        set(p, "contentDisposition", headers.contentDisposition());
        set(p, "contentEncoding", headers.contentEncoding());
        set(p, "contentLanguage", headers.contentLanguage());
        set(p, "expires", headers.expires());
        headers.userMetadata().forEach((name, value) -> p.setProperty("userMetadata." + encode(name), value));
        write(file(bucket, key), p);
    }

    /**
     * Records the expiry time for a temporary restored copy.
     */
    public void restored(String bucket, String key, int days) {
        Path path = file(bucket, key);
        Properties p = new Properties();
        if (Files.exists(path))try (InputStream in = Files.newInputStream(path)) {
            p.load(in);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        p.setProperty("restoreExpiry", Instant.now().plus(Duration.ofDays(Math.max(1, days))).toString());
        write(path, p);
    } 

    /**
     * Deletes only the centralized metadata record for an object.
     */
    public void delete(String bucket, String key) {
        try {
            Files.deleteIfExists(file(bucket, key));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Resolves the safe centralized metadata path for one logical object.
     */
    private Path file(String bucket, String key) {
        return properties.getNearlineRoot().toAbsolutePath().resolve(".gateway-metadata").
                resolve(encodedCategory(store.category(bucket))).
                resolve(Base64.getUrlEncoder().withoutPadding().
                        encodeToString(key.getBytes(java.nio.charset.StandardCharsets.UTF_8)) + ".properties");
    }

    private String encodedCategory(String category) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(category.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), java.nio.charset.StandardCharsets.UTF_8);
    }

    private void set(Properties p, String name, String value) {
        if (value != null && !value.isBlank()) {
            p.setProperty(name, value);

        }
    }

    /**
     * Creates parent directories and writes the Java properties document.
     */
    private void write(Path path, Properties p) {
        try {
            Files.createDirectories(path.getParent());
            try (OutputStream out = Files.newOutputStream(path)) {
                p.store(out, "gateway object metadata");
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * HTTP representation headers persisted for later GET and HEAD responses.
     */
    public record ObjectHeaders(String contentType, String cacheControl, String contentDisposition, String contentEncoding, String contentLanguage, String expires, Map<String, String> userMetadata) {

        public ObjectHeaders {
            // Defensive copying prevents metadata changes after validation.
            userMetadata = userMetadata == null ? Map.of() : Map.copyOf(userMetadata);

            // S3 limits user-controlled metadata names and values to 2 KiB.
            int bytes = userMetadata.entrySet().stream().mapToInt(entry -> utf8Length(entry.getKey()) + utf8Length(entry.getValue())).sum();
            if (bytes > 2048) {
                throw new S3Exception(400, "MetadataTooLarge", "Your metadata headers exceed the maximum allowed metadata size", "x-amz-meta-*");
            }
        }

        private static int utf8Length(String value) {
            return value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
    }

    /**
     * Complete metadata returned to the controller for one object.
     */
    public record Metadata(String storageClass, String restoreExpiry, long contentLength, String etag, Instant lastModified, ObjectHeaders headers) {

        public String contentType() {
            return headers.contentType();
        }
    }

    private long parseContentLength(String value) {
        if (value == null || value.isBlank()) {
            return -1;
        }

        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            return -1;
        }
    }

    private Instant parseInstant(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        try {
            return Instant.parse(value);
        } catch (java.time.format.DateTimeParseException exception) {
            return null;
        }
    }
}
