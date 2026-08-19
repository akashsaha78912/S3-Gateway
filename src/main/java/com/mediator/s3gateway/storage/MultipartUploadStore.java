package com.mediator.s3gateway.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.springframework.stereotype.Service;

import com.mediator.s3gateway.config.GatewayProperties;
import com.mediator.s3gateway.exception.S3Exception;

@Service
public class MultipartUploadStore {

    private final GatewayProperties properties;
    private final NearlineStore store;

    public MultipartUploadStore(GatewayProperties properties, NearlineStore store) {
        this.properties = properties;
        this.store = store;
    }

    //create a new multipart upload and return the upload ID
    public String initiate(String bucket, String key, String storageClass) throws IOException {
        store.object(bucket, key);

        String uploadId = UUID.randomUUID().toString();
        Path uploadDir = uploadDir(uploadId);
        Files.createDirectories(partsDir(uploadId));

        Properties state = new Properties();
        state.setProperty("bucket", bucket);
        state.setProperty("key", key);
        state.setProperty("storageClass", storageClass);
        try (OutputStream out = Files.newOutputStream(uploadDir.resolve("upload.properties"))) {
            state.store(out, "Multipart upload state");
        }
        return uploadId;

    }

    //get the state of a multipart upload
    // public String putPart(String uploadId, String bucket, String key, int partNumber, InputStream input, long expectedLength) throws IOException {

    //     validatePartNumber(partNumber);
    //     Properties state = state(uploadId);
    //     validateTarget(state, bucket, key);

    //     Path partPath = partsDir(uploadId).resolve(partName(partNumber));
    //     MessageDigest md5 = md5();
    //     long written = 0;

    //     try (InputStream in = input; OutputStream out = Files.newOutputStream(partPath)) {
    //         byte[] buffer = new byte[124 * 128];
    //         int n;
    //         while ((n = in.read(buffer)) != -1) {
    //             out.write(buffer, 0, n);
    //             md5.update(buffer, 0, n);
    //             written += n;

    //         }
    //     }
    //     if (expectedLength >= 0 && expectedLength != written) {
    //         Files.deleteIfExists(partPath);
    //         throw new S3Exception(400, "IncompleteBody", "The receive body length does not match with content-length", key);

    //     }
    //     return hex(md5.digest());
    // }
public String putPart(
        String uploadId,
        String bucket,
        String key,
        int partNumber,
        InputStream input,
        long expectedLength,
        String suppliedContentMd5
) throws IOException {

    validatePartNumber(partNumber);

    Properties state = state(uploadId);
    validateTarget(state, bucket, key);

    Path partPath =
            partsDir(uploadId).resolve(partName(partNumber));

    MessageDigest md5 = md5();
    long written = 0;

    try (InputStream in = input;
         OutputStream out = Files.newOutputStream(partPath)) {

        byte[] buffer = new byte[128 * 1024];
        int n;

        while ((n = in.read(buffer)) != -1) {
            out.write(buffer, 0, n);
            md5.update(buffer, 0, n);
            written += n;
        }
    } catch (IOException | RuntimeException exception) {
        Files.deleteIfExists(partPath);
        throw exception;
    }

    if (expectedLength >= 0 && expectedLength != written) {
        Files.deleteIfExists(partPath);

        throw new S3Exception(
                400,
                "IncompleteBody",
                "The received body length does not match Content-Length",
                key
        );
    }

    byte[] calculatedMd5 = md5.digest();

    if (suppliedContentMd5 != null
            && !suppliedContentMd5.isBlank()) {

        final byte[] suppliedMd5;

        try {
            suppliedMd5 = Base64.getDecoder().decode(
                    suppliedContentMd5.trim()
            );
        } catch (IllegalArgumentException exception) {
            Files.deleteIfExists(partPath);

            throw new S3Exception(
                    400,
                    "InvalidDigest",
                    "The Content-MD5 value is not valid Base64",
                    key
            );
        }

        if (suppliedMd5.length != 16) {
            Files.deleteIfExists(partPath);

            throw new S3Exception(
                    400,
                    "InvalidDigest",
                    "The Content-MD5 value is not a valid MD5 digest",
                    key
            );
        }

        if (!MessageDigest.isEqual(
                suppliedMd5,
                calculatedMd5
        )) {
            Files.deleteIfExists(partPath);

            throw new S3Exception(
                    400,
                    "BadDigest",
                    "The Content-MD5 you specified did not match what was received",
                    key
            );
        }
    }

    return hex(calculatedMd5);
}
    //complete a multipart upload and return the assembled file
    // public CompletedMultipart complete(String uploadId, String bucket, String key, List<Integer> requestedParts) throws IOException {
    //     Properties state = state(uploadId);
    //     validateTarget(state, bucket, key);
    //     if (requestedParts.isEmpty()) {
    //         throw new S3Exception(400, "MalformedXML", "Complete multipart upload request must contain at least one part", key);
    //     }
    //     Path assembled = uploadDir(uploadId).resolve("assembled.object");
    //     MessageDigest md5 = md5();
    //     long total = 0;
    //     try (OutputStream out = Files.newOutputStream(assembled)) {
    //         byte[] buffer = new byte[1024 * 128];
    //         for (Integer partNumber : requestedParts) {
    //             validatePartNumber(partNumber);
    //             Path partPath = partsDir(uploadId).resolve(partName(partNumber));
    //             if (!Files.isRegularFile(partPath)) {
    //                 throw new S3Exception(400, "InvalidPart", "One or more specified parts could not be found", key);
    //             }
    //             try (InputStream in = Files.newInputStream(partPath)) {
    //                 int n;
    //                 while ((n = in.read(buffer)) != -1) {
    //                     out.write(buffer, 0, n);
    //                     md5.update(buffer, 0, n);
    //                     total += n;
    //                 }
    //             }
    //         }
    //     }
    //     return new CompletedMultipart(assembled, total, state.getProperty("storageClass", "STANDARD"), hex(md5.digest()));
    // }
    public CompletedMultipart complete(
            String uploadId,
            String bucket,
            String key,
            List<MultipartUploadStore.CompletedPart> requestedParts
    ) throws IOException {

        Properties state = state(uploadId);
        validateTarget(state, bucket, key);

        if (requestedParts.isEmpty()) {
            throw new S3Exception(
                    400,
                    "MalformedXML",
                    "Complete multipart upload request must contain at least one part",
                    key
            );
        }

        Path assembled
                = uploadDir(uploadId).resolve("assembled.object");

        /*
     * AWS multipart ETag:
     * MD5(concatenated binary MD5 values of every part) + "-partCount"
         */
        MessageDigest multipartDigest = md5();
        long total = 0;

        try (OutputStream out = Files.newOutputStream(assembled)) {
            byte[] buffer = new byte[1024 * 128];

      for (CompletedPart requestedPart : requestedParts) {
    int partNumber = requestedPart.partNumber();
    validatePartNumber(partNumber);

    Path partPath =
            partsDir(uploadId).resolve(partName(partNumber));

                if (!Files.isRegularFile(partPath)) {
                    throw new S3Exception(
                            400,
                            "InvalidPart",
                            "One or more specified parts could not be found",
                            key
                    );
                }

                MessageDigest partDigest = md5();

                try (InputStream in
                        = Files.newInputStream(partPath)) {

                    int n;

                    while ((n = in.read(buffer)) != -1) {
                        out.write(buffer, 0, n);

                        // Calculate the MD5 of this individual part.
                        partDigest.update(buffer, 0, n);

                        total += n;
                    }
                }

                /*
             * Add the raw 16-byte part MD5—not its hexadecimal text—to the
             * combined multipart digest.
                 */
                byte[] calculatedPartMd5 = partDigest.digest();
String calculatedPartEtag = hex(calculatedPartMd5);

String suppliedPartEtag =
        normalizeEtag(requestedPart.etag());

if (!calculatedPartEtag.equalsIgnoreCase(suppliedPartEtag)) {
    throw new S3Exception(
            400,
            "InvalidPart",
            "The ETag for part " + partNumber
                    + " does not match the uploaded part",
            key
    );
}

multipartDigest.update(calculatedPartMd5);
            }
        } catch (IOException | RuntimeException exception) {
            Files.deleteIfExists(assembled);
            throw exception;
        }

        String multipartEtag
                = hex(multipartDigest.digest())
                + "-"
                + requestedParts.size();

        return new CompletedMultipart(
                assembled,
                total,
                state.getProperty("storageClass", "STANDARD"),
                multipartEtag
        );
    }
private static String normalizeEtag(String etag) {
    if (etag == null) {
        return "";
    }

    String value = etag.trim();

    if (value.startsWith("\"")
            && value.endsWith("\"")
            && value.length() >= 2) {
        value = value.substring(1, value.length() - 1);
    }

    return value;
}
    //abort a multipart upload and clean up any stored parts
    public void abort(String uploadId, String bucket, String key) throws IOException {
        Properties state = state(uploadId);
        validateTarget(state, bucket, key);
        cleanup(uploadId);
    }

    //clean up the temporary files associated with a multipart upload
    public void cleanup(String uploadId) throws IOException {
        Path dir = uploadDir(uploadId);
        if (!Files.exists(dir)) {
            return;
        }

        try (var paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }

            });
        }
    }
   
    //get the state of a multipart upload
    private Properties state(String uploadId) throws IOException {
        Path file = uploadDir(uploadId).resolve("upload.properties");
        if (!Files.isRegularFile(file)) {
            throw new S3Exception(404, "NoSuchUpload", "The specified multipart upload does not exist", uploadId);
        }
        Properties state = new Properties();
        try (InputStream in = Files.newInputStream(file)) {
            state.load(in);
        }
        return state;
    }

    //validate that the bucket and key match the stored state of the multipart upload
    private void validateTarget(Properties state, String bucket, String key) {
        if (!bucket.equals(state.getProperty("bucket")) || !key.equals(state.getProperty("key"))) {
            throw new S3Exception(400, "InvalidTarget", "The specified bucket and key do not match the multipart upload", key);
        }
    }
    //get the directory for a multipart upload
    private Path uploadDir(String uploadId) {
        return properties.getNearlineRoot().toAbsolutePath().normalize()
                .resolve(".gateway-multipart")
                .resolve(uploadId)
                .normalize();
    }

    //get the directory for the parts of a multipart upload
    private Path partsDir(String uploadId) {
        return uploadDir(uploadId).resolve("parts");
    }

    //get the name of a part file for a given part number
    private String partName(int partNumber) {
        return String.format("%06d.part", partNumber);
    }

    //validate that a part number is between 1 and 10,000
    private static void validatePartNumber(int partNumber) {
        if (partNumber < 1 || partNumber > 10000) {
            throw new S3Exception(400, "InvalidArgument", "Part number must be between 1 and 10000", String.valueOf(partNumber));

        }
    }
    
    //get the MD5 digest for a given input stream
    private static MessageDigest md5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available", e);
        }
    }

    //convert a byte array to a hexadecimal string
    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder();
        for (byte b : bytes) {
            value.append(String.format("%02x", b));
        }
        return value.toString();
    }

    public record CompletedMultipart(Path assembledFile, long length, String storageClass, String etag) {

    }
public record CompletedPart(
        int partNumber,
        String etag
) {
}
}
