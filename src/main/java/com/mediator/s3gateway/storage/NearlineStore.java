package com.mediator.s3gateway.storage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.mediator.s3gateway.config.GatewayProperties;
import com.mediator.s3gateway.exception.S3Exception;

/**
 * Owns physical bucket directories and object files on the nearline disk.
 *
 * <p>
 * It resolves bucket aliases to categories, prevents path traversal, validates
 * checksums and publishes completed uploads atomically when possible.
 */
@Service
public class NearlineStore {

    private final GatewayProperties properties;
    private final Map<Path, Object> objectLocks = new ConcurrentHashMap<>();
    private final Logger log = LoggerFactory.getLogger(NearlineStore.class);

    /**
     * Creates the store from the configured NLD root and bucket mappings.
     */
    public NearlineStore(GatewayProperties properties) {
        this.properties = properties;
    }

    /**
     * Resolves a public bucket name to its physical NLD category.
     */
    public String category(String bucket) {
        validateBucket(bucket);
        String category = properties.getBuckets().getOrDefault(bucket, bucket);
        //  String category = properties.getBuckets().get(bucket);

        //     if (category == null) {
        //         category = dynamicCategory(bucket);
        //     }
        //    Path categoryDir = categoryPath(category);
        //     if (!Files.exists(categoryDir)) {
        //         throw new S3Exception(404, "NoSuchBucket", "The specified bucket does not exist", bucket);
        //     }
        return category;
    }

    /**
     * Lists bucket aliases from configuration and visible NLD directories.
     */
    public Set<String> buckets() {
        Set<String> result = new TreeSet<>(properties.getBuckets().keySet());
        Path root = properties.getNearlineRoot().toAbsolutePath().normalize();

        if (Files.isDirectory(root)) {
            try (Stream<Path> stream = Files.list(root)) {
                stream.filter(Files::isDirectory)
                        .map(p -> p.getFileName().toString())
                        .filter(name -> !name.startsWith("."))
                        .forEach(category -> {
                            // Reverse lookup in properties, or format directory back to S3 bucket name format
                            String matchedBucket = properties.getBuckets().entrySet().stream()
                                    .filter(e -> e.getValue().equalsIgnoreCase(category))
                                    .map(Map.Entry::getKey)
                                    .findFirst()
                                    .orElseGet(() -> category.toLowerCase(Locale.ROOT).replace('_', '-'));

                            result.add(matchedBucket);
                        });
            } catch (IOException e) {
                // Ignore unreadable root on list
            }
        }
        return Collections.unmodifiableSet(result);
    }

    /**
     * Creates a bucket by creating its category directory on the NLD.
     */
    public synchronized String createBucket(String bucket) throws IOException {
        validateBucket(bucket);
        // String category = properties.getBuckets().getOrDefault(bucket, dynamicCategory(bucket));
        String category = properties.getBuckets().getOrDefault(bucket, bucket);
        Path directory = categoryPath(category);

        if (Files.exists(directory)) {
            throw new S3Exception(409, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it", bucket);
        }

        // Directly create directory on NLD disk if not present
        Files.createDirectories(directory);
        return category;
    }

    /**
     * Deletes an empty bucket directory and rejects non-empty buckets.
     */
    public synchronized void deleteBucket(String bucket) throws IOException {
        String category = category(bucket);
        Path categoryDir = categoryPath(category);

        if (Files.exists(categoryDir)) {
            try (Stream<Path> s = Files.list(categoryDir)) {
                if (s.findAny().isPresent()) {
                    throw new S3Exception(409, "BucketNotEmpty", "The bucket you tried to delete is not empty", bucket);
                }
            }
            Files.deleteIfExists(categoryDir);
        }
    }

    /**
     * Deletes an existing object resolved from its bucket alias and key.
     */
    public void delete(String bucket, String key) throws IOException {
        Files.deleteIfExists(existing(bucket, key));
    }

    /**
     * Deletes an object when the caller already has its category name.
     */
    public void deleteObject(String category, String key) throws IOException {
        Path base = categoryPath(category);
        Path result = base.resolve(key).normalize();
        if (result.startsWith(base)) {
            Files.deleteIfExists(result);
        }
    }

    /**
     * Validates the basic lowercase S3 bucket-name shape and length.
     */
    private void validateBucket(String bucket) {
        if (bucket == null || !bucket.matches("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")) {
            throw new S3Exception(400, "InvalidBucketName", "The specified bucket is not valid", bucket);
        }
    }

    /**
     * Converts an unmapped bucket name into its default category form.
     */
    // private String dynamicCategory(String bucket) {
    //     return bucket.toUpperCase(Locale.ROOT).replace('-', '_').replace('.', '_');
    // }
    /**
     * Resolves a category below the NLD root and rejects escaping paths.
     */
    private Path categoryPath(String category) {
        Path root = properties.getNearlineRoot().toAbsolutePath().normalize();
        Path result = root.resolve(category).normalize();
        if (!result.startsWith(root) || result.equals(root)) {
            throw new IllegalStateException("Configured category escapes the nearline root: " + category);
        }
        return result;
    }

    /**
     * Resolves the physical category root for a public bucket alias.
     */
    Path categoryRoot(String bucket) {
        return categoryPath(category(bucket));
    }

    /**
     * Resolves an object path and rejects empty keys or path traversal.
     */
    public Path object(String bucket, String key) {
        if (key == null || key.isBlank()) {
            throw new S3Exception(400, "InvalidRequest", "An object key is required", bucket);
        }
        Path base = categoryRoot(bucket);
        Path result = base.resolve(key).normalize();
        if (!result.startsWith(base)) {
            throw new S3Exception(400, "InvalidURI", "Object key is invalid", key);
        }
        return result;
    }

    /**
     * Resolves an object and requires it to exist as a regular file.
     */
    public Path existing(String bucket, String key) {
        Path path = object(bucket, key);
        if (!Files.isRegularFile(path)) {
            throw new S3Exception(404, "NoSuchKey", "The specified key does not exist", key);
        }
        return path;
    }

    /**
     * Convenience PUT without checksums or conditional headers.
     */
    public Stored put(String bucket, String key, InputStream input, long expectedLength) throws IOException {
        return put(bucket, key, input, expectedLength, Map.of(), null, null);
    }

    /**
     * Convenience PUT with checksums but no conditional headers.
     */
    public Stored put(String bucket, String key, InputStream input, long expectedLength, Map<String, String> clientChecksums) throws IOException {
        return put(bucket, key, input, expectedLength, clientChecksums, null, null);
    }

    /**
     * Stores one object with optional checksums and write preconditions.
     */
    public Stored put(String bucket, String key, InputStream input, long expectedLength, Map<String, String> clientChecksums, String ifMatch, String ifNoneMatch) throws IOException {
        validateChecksumHeaders(clientChecksums, key);

        Path destination = object(bucket, key);
        Path parentDir = destination.getParent();

        // Check if missing on NLD disk, then create directory tree
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }

        // Serialize condition evaluation and replacement for this object path.
        Object lock = objectLocks.computeIfAbsent(destination, p -> new Object());
        synchronized (lock) {
            validateWriteConditions(destination, key, ifMatch, ifNoneMatch);
            return writeObject(destination, key, input, expectedLength, clientChecksums);
        }
    }

    /**
     * Streams to a staging file, verifies the content, then publishes it.
     */
    private Stored writeObject(Path destination, String key, InputStream input, long expectedLength, Map<String, String> clientChecksums) throws IOException {
        Path staging = destination.getParent().resolve("." + destination.getFileName() + "." + UUID.randomUUID() + ".uploading");
        long written = 0;
        MessageDigest digest = messageDigest("MD5");
        MessageDigest sha1 = clientChecksums.containsKey("x-amz-checksum-sha1") ? messageDigest("SHA-1") : null;
        MessageDigest sha256 = clientChecksums.containsKey("x-amz-checksum-sha256") ? messageDigest("SHA-256") : null;
        MessageDigest sha512 = clientChecksums.containsKey("x-amz-checksum-sha512") ? messageDigest("SHA-512") : null;
        Checksum crc32 = clientChecksums.containsKey("x-amz-checksum-crc32") ? new CRC32() : null;
        Checksum crc32c = clientChecksums.containsKey("x-amz-checksum-crc32c") ? new CRC32C() : null;

        // Calculate all requested checksums during the same pass that saves bytes.
        try (InputStream in = input; OutputStream out = Files.newOutputStream(staging, StandardOpenOption.CREATE_NEW)) {
            byte[] buffer = new byte[1024 * 128];
            int n;
            long lastLoggedPercent = -1;
            while ((n = in.read(buffer)) != -1) {

                out.write(buffer, 0, n);
                digest.update(buffer, 0, n);
                if (sha1 != null) {
                    sha1.update(buffer, 0, n);
                }
                if (sha256 != null) {
                    sha256.update(buffer, 0, n);
                }
                if (sha512 != null) {
                    sha512.update(buffer, 0, n);
                }
                if (crc32 != null) {
                    crc32.update(buffer, 0, n);
                }
                if (crc32c != null) {
                    crc32c.update(buffer, 0, n);
                }
                written += n;

                if (expectedLength > 0) {
                    long percent = Math.min(100, written * 100 / expectedLength);

                    // Log only at each new 5% boundary
                    if (percent / 5 > lastLoggedPercent / 5) {
                        log.info(
                                "PUT progress: key={}, written={} bytes, total={} bytes, progress={}%",
                                key, written, expectedLength, percent
                        );
                        lastLoggedPercent = percent;
                    }
                } else {
                    log.info("PUT progress: key={}, written={} bytes", key, written);
                }
            }
        } catch (Exception e) {
            Files.deleteIfExists(staging);
            throw e;
        }

        if (expectedLength >= 0 && expectedLength != written) {
            Files.deleteIfExists(staging);
            throw new S3Exception(400, "IncompleteBody", "The received body length does not match Content-Length", key);
        }

        byte[] md5 = digest.digest();
        Map<String, byte[]> calculated = new HashMap<>();
        calculated.put("content-md5", md5);
        // calculated.put("x-amz-checksum-md5", md5);
        if (sha1 != null) {
            calculated.put("x-amz-checksum-sha1", sha1.digest());
        }
        if (sha256 != null) {
            calculated.put("x-amz-checksum-sha256", sha256.digest());
        }
        if (sha512 != null) {
            calculated.put("x-amz-checksum-sha512", sha512.digest());
        }
        if (crc32 != null) {
            calculated.put("x-amz-checksum-crc32", checksumBytes(crc32));
        }
        if (crc32c != null) {
            calculated.put("x-amz-checksum-crc32c", checksumBytes(crc32c));
        }

        try {
            verifyChecksums(clientChecksums, calculated, key);
        } catch (S3Exception e) {
            Files.deleteIfExists(staging);
            throw e;
        }

        Map<String, String> responseChecksums = new LinkedHashMap<>();
        clientChecksums.keySet().stream()
                .filter(name -> name.startsWith("x-amz-checksum-"))
                .forEach(name -> responseChecksums.put(name, Base64.getEncoder().encodeToString(calculated.get(name))));

        try {
            Files.move(staging, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(staging, destination, StandardCopyOption.REPLACE_EXISTING);
        }

        return new Stored(destination, written, hex(md5), Files.getLastModifiedTime(destination), responseChecksums);
    }

    /**
     * Recursively lists object files, optionally filtered by key prefix.
     */
    public List<Entry> list(String bucket, String prefix) throws IOException {
        Path root = categoryRoot(bucket);
        if (!Files.exists(root)) {
            return List.of();
        }
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile).map(p -> {
                try {
                    return new Entry(root.relativize(p).toString().replace(File.separatorChar, '/'), Files.size(p), Files.getLastModifiedTime(p));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            })
                    .filter(e -> !e.key().startsWith(".gateway-multipart/"))
                    .filter(e -> prefix == null || e.key().startsWith(prefix))
                    .sorted(Comparator.comparing(Entry::key))
                    .toList();
        }
    }

    /**
     * Result returned after a successful NLD object write.
     */
    public record Stored(Path path, long length, String etag, FileTime lastModified, Map<String, String> checksums) {

        public Stored {
            checksums = Map.copyOf(checksums);
        }
    }

    /**
     * Minimal object information consumed by ListObjectsV2.
     */
    public record Entry(String key, long length, FileTime lastModified) {

    }

    /**
     * Obtains a digest algorithm that is required from the JDK.
     */
    private static MessageDigest messageDigest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Validates checksum Base64 syntax and decoded byte lengths.
     */
    private static void validateChecksumHeaders(Map<String, String> checksums, String key) {
        Map<String, Integer> lengths = Map.of(
                "content-md5", 16,
                "x-amz-checksum-md5", 16,
                "x-amz-checksum-crc32", 4,
                "x-amz-checksum-crc32c", 4,
                "x-amz-checksum-sha1", 20,
                "x-amz-checksum-sha256", 32,
                "x-amz-checksum-sha512", 64
        );
        checksums.forEach((name, value) -> {
            try {
                byte[] decoded = Base64.getDecoder().decode(value);
                if (decoded.length != lengths.getOrDefault(name, -1)) {
                    throw new IllegalArgumentException();
                }
            } catch (IllegalArgumentException e) {
                throw new S3Exception(400, "InvalidDigest", "The checksum value specified is not valid", key);
            }
        });
    }

    /**
     * Compares client checksum values with checksums of received bytes.
     */
    private static void verifyChecksums(Map<String, String> expected, Map<String, byte[]> calculated, String key) {
        expected.forEach((name, value) -> {
            byte[] supplied = Base64.getDecoder().decode(value);
            if (!MessageDigest.isEqual(supplied, calculated.get(name))) {
                throw new S3Exception(400, "BadDigest", "The checksum value specified did not match what was received", key);
            }
        });
    }

    /**
     * Applies PutObject If-Match and If-None-Match conditions.
     */
    private static void validateWriteConditions(Path destination, String key, String ifMatch, String ifNoneMatch) throws IOException {
        boolean exists = Files.isRegularFile(destination);
        if (ifMatch != null && !ifMatch.isBlank()) {
            if (!exists) {
                throw new S3Exception(404, "NoSuchKey", "The specified key does not exist", key);
            }
            String supplied = normalizeEtag(ifMatch);
            if (!"*".equals(supplied) && !MessageDigest.isEqual(supplied.getBytes(java.nio.charset.StandardCharsets.US_ASCII), fileEtag(destination).getBytes(java.nio.charset.StandardCharsets.US_ASCII))) {
                throw new S3Exception(412, "PreconditionFailed", "At least one of the preconditions you specified did not hold", key);
            }
        }
        if (ifNoneMatch != null && !ifNoneMatch.isBlank()) {
            if (!"*".equals(ifNoneMatch.trim())) {
                throw new S3Exception(400, "InvalidRequest", "If-None-Match for PutObject must be '*'", key);
            }
            if (exists) {
                throw new S3Exception(412, "PreconditionFailed", "At least one of the preconditions you specified did not hold", key);
            }
        }
    }

    /**
     * Removes weak-validator syntax and quotes from an ETag.
     */
    private static String normalizeEtag(String value) {
        String result = value.trim();
        if (result.startsWith("W/")) {
            result = result.substring(2).trim();
        }
        if (result.length() >= 2 && result.startsWith("\"") && result.endsWith("\"")) {
            result = result.substring(1, result.length() - 1);
        }
        return result;
    }

    /**
     * Calculates the MD5-based ETag of an existing NLD file.
     */
    private static String fileEtag(Path path) throws IOException {
        MessageDigest digest = messageDigest("MD5");
        try (InputStream in = Files.newInputStream(path)) {
            byte[] buffer = new byte[1024 * 128];
            int n;
            while ((n = in.read(buffer)) != -1) {
                digest.update(buffer, 0, n);
            }
        }
        return hex(digest.digest());
    }

    /**
     * Converts a 32-bit CRC value into network-order bytes.
     */
    private static byte[] checksumBytes(Checksum checksum) {
        long value = checksum.getValue();
        return new byte[]{(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
    }

    /**
     * Renders digest bytes as lowercase hexadecimal text.
     */
    private static String hex(byte[] bytes) {
        StringBuilder s = new StringBuilder();
        for (byte b : bytes) {
            s.append(String.format("%02x", b));
        }
        return s.toString();
    }
}
