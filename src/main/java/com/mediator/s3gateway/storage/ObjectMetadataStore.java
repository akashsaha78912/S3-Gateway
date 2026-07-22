package com.mediator.s3gateway.storage;

import com.mediator.s3gateway.config.GatewayProperties;
import com.mediator.s3gateway.exception.S3Exception;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

@Service
public class ObjectMetadataStore {

    private static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    private final GatewayProperties properties;
    private final NearlineStore store;

    public ObjectMetadataStore(GatewayProperties properties, NearlineStore store) {
        this.properties = properties;
        this.store = store;
    }

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
        return new Metadata(p.getProperty("storageClass", "STANDARD"), p.getProperty("restoreExpiry"), headers);
    }

    public void put(String bucket, String key, String storageClass, ObjectHeaders headers) {
        Properties p = new Properties();
        p.setProperty("storageClass", storageClass);
        p.setProperty("contentType", headers.contentType() == null || headers.contentType().isBlank() ? DEFAULT_CONTENT_TYPE : headers.contentType());
        set(p, "cacheControl", headers.cacheControl());
        set(p, "contentDisposition", headers.contentDisposition());
        set(p, "contentEncoding", headers.contentEncoding());
        set(p, "contentLanguage", headers.contentLanguage());
        set(p, "expires", headers.expires());
        headers.userMetadata().forEach((name, value) -> p.setProperty("userMetadata." + encode(name), value));
        write(file(bucket, key), p);
    }

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

    public void delete(String bucket, String key) {
        try {
            Files.deleteIfExists(file(bucket, key));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Path file(String bucket, String key) {
        return properties.getNearlineRoot().toAbsolutePath().resolve(".gateway-metadata").resolve(encodedCategory(store.category(bucket))).resolve(Base64.getUrlEncoder().withoutPadding().encodeToString(key.getBytes(java.nio.charset.StandardCharsets.UTF_8)) + ".properties");
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

    public record ObjectHeaders(String contentType, String cacheControl, String contentDisposition, String contentEncoding, String contentLanguage, String expires, Map<String, String> userMetadata) {

        public ObjectHeaders {
            userMetadata = userMetadata == null ? Map.of() : Map.copyOf(userMetadata);
            int bytes = userMetadata.entrySet().stream().mapToInt(entry -> utf8Length(entry.getKey()) + utf8Length(entry.getValue())).sum();
            if (bytes > 2048) {
                throw new S3Exception(400, "MetadataTooLarge", "Your metadata headers exceed the maximum allowed metadata size", "x-amz-meta-*");
            }
        }

        private static int utf8Length(String value) {
            return value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
    }

    public record Metadata(String storageClass, String restoreExpiry, ObjectHeaders headers) {

        public String contentType() {
            return headers.contentType();
        }
    }
}
