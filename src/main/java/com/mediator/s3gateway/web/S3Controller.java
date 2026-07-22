package com.mediator.s3gateway.web;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mediator.s3gateway.exception.S3Exception;
import com.mediator.s3gateway.integration.RequestRegistry;
import com.mediator.s3gateway.storage.NearlineStore;
import com.mediator.s3gateway.storage.ObjectMetadataStore;

import jakarta.servlet.http.HttpServletRequest;

@RestController
public class S3Controller {

    private final NearlineStore store;
    private final RequestRegistry requests;
    private final ObjectMetadataStore metadata;

    public S3Controller(NearlineStore store, RequestRegistry requests, ObjectMetadataStore metadata) {
        this.store = store;
        this.requests = requests;
        this.metadata = metadata;
    }

    @GetMapping(value = "/", produces = MediaType.APPLICATION_XML_VALUE)//List of Buckets
    public String listBuckets() {
        StringBuilder x = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Buckets>");
        for (String b : store.buckets()) {
            x.append("<Bucket><Name>").append(xml(b)).append("</Name></Bucket>");
        }
        return x.append("</Buckets></ListAllMyBucketsResult>").toString();
    }

    @PutMapping("/{bucket}") //create bucket
    public ResponseEntity<Void> createBucket(@PathVariable String bucket) throws IOException {
        store.createBucket(bucket);
        return ResponseEntity.ok().header(HttpHeaders.LOCATION, "/" + bucket).build();
    }

    @DeleteMapping("/{bucket}")//delete bucket disable as for now
    public ResponseEntity<String> deleteBucket(@PathVariable String bucket) {
        String xmlError = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<Error>"
                + "<Code>AccessDenied</Code>"
                + "<Message>Bucket deletion is not supported by Nearline Gateway.</Message>"
                + "<Resource>/" + (bucket != null ? bucket : "") + "</Resource>"
                + "<RequestId>44442222AAAA2222</RequestId>"
                + "</Error>";

        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .contentType(MediaType.APPLICATION_XML)
                .body(xmlError);
    }

    @DeleteMapping("/{bucket}/{*key}")//delete object disable as for now
    public ResponseEntity<String> deleteObject(@PathVariable String bucket, @PathVariable String key) {
        String actual = clean(key);

        String xmlError = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<Error>"
                + "<Code>NotImplemented</Code>"
                + "<Message>DeleteObject operation is not supported by Nearline Gateway.</Message>"
                + "<Resource>/" + (bucket != null ? bucket : "") + "/" + actual + "</Resource>"
                + "<RequestId>44442222AAAA2222</RequestId>"
                + "</Error>";

        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
                .contentType(MediaType.APPLICATION_XML)
                .body(xmlError);
    }

    @GetMapping(value = "/{bucket}", params = "list-type=2", produces = MediaType.APPLICATION_XML_VALUE)// list objects in bucket v2
    public String list(@PathVariable String bucket,
            @RequestParam(required = false) String prefix,
            @RequestParam(required = false) String delimiter,
            @RequestParam(defaultValue = "1000") int maxKeys,
            @RequestParam(required = false, name = "start-after") String after,
            @RequestParam(required = false, name = "continuation-token") String token) throws IOException {
        store.category(bucket);
        String pfx = prefix == null ? "" : prefix;
        List<NearlineStore.Entry> all = store.list(bucket, prefix).stream()
                .filter(v -> after == null || v.key().compareTo(after) > 0)
                .toList();

        int start = token == null ? 0 : Integer.parseInt(new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8));
        int limit = Math.max(1, Math.min(maxKeys, 1000));

        Set<String> commonPrefixes = new TreeSet<>();
        List<NearlineStore.Entry> filteredItems = new java.util.ArrayList<>();

        // Group entries into CommonPrefixes if delimiter is present
        for (var e : all) {
            String relativeKey = e.key().startsWith(pfx) ? e.key().substring(pfx.length()) : e.key();
            if (delimiter != null && !delimiter.isEmpty() && relativeKey.contains(delimiter)) {
                String subPrefix = pfx + relativeKey.substring(0, relativeKey.indexOf(delimiter) + delimiter.length());
                commonPrefixes.add(subPrefix);
            } else {
                filteredItems.add(e);
            }
        }

        List<NearlineStore.Entry> items = filteredItems.subList(Math.min(start, filteredItems.size()), Math.min(start + limit, filteredItems.size()));
        boolean truncated = start + items.size() < filteredItems.size();

        StringBuilder x = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>")
                .append(xml(bucket)).append("</Name><Prefix>").append(xml(prefix)).append("</Prefix><KeyCount>")
                .append(items.size() + commonPrefixes.size()).append("</KeyCount><MaxKeys>").append(limit).append("</MaxKeys><IsTruncated>")
                .append(truncated).append("</IsTruncated>");

        for (var e : items) {
            String etag = metadata.get(bucket, e.key()) != null ? "\"" + e.key().hashCode() + "\"" : "\"\"";
            x.append("<Contents><Key>").append(xml(e.key())).append("</Key><LastModified>").append(time(e.lastModified()))
                    .append("</LastModified><ETag>").append(etag).append("</ETag><Size>").append(e.length()).append("</Size><StorageClass>")
                    .append(metadata.get(bucket, e.key()).storageClass()).append("</StorageClass></Contents>");
        }

        for (String cp : commonPrefixes) {
            x.append("<CommonPrefixes><Prefix>").append(xml(cp)).append("</Prefix></CommonPrefixes>");
        }

        if (truncated) {
            x.append("<NextContinuationToken>").append(Base64.getUrlEncoder().encodeToString(Integer.toString(start + items.size()).getBytes(StandardCharsets.UTF_8))).append("</NextContinuationToken>");
        }
        return x.append("</ListBucketResult>").toString();
    }

    @RequestMapping(value = "/{bucket}/{*key}", method = RequestMethod.HEAD)// head object
    public ResponseEntity<Void> head(@PathVariable String bucket, @PathVariable String key) throws IOException {
        String actual = clean(key);
        Path p = store.existing(bucket, actual);
        return headers(p, metadata.get(bucket, actual)).build();
    }

    @GetMapping("/{bucket}/{*key}")//get object after restoring
    public ResponseEntity<InputStreamResource> get(@PathVariable String bucket, @PathVariable String key, @RequestHeader(value = "Range", required = false) String range) throws IOException {
        Path p = store.existing(bucket, clean(key));
        long size = Files.size(p), start = 0, end = size - 1;
        HttpStatus status = HttpStatus.OK;
        if (range != null) {
            long[] parsed = parseRange(range, size);
            start = parsed[0];
            end = parsed[1];
            status = HttpStatus.PARTIAL_CONTENT;
        }
        InputStream in = Files.newInputStream(p);
        in.skipNBytes(start);
        long count = end - start + 1;
        HttpHeaders h = new HttpHeaders();
        h.setContentLength(count);
        h.setLastModified(Files.getLastModifiedTime(p).toMillis());
        ObjectMetadataStore.Metadata m = metadata.get(bucket, clean(key));
        applyObjectHeaders(h, m);
        h.set(HttpHeaders.ACCEPT_RANGES, "bytes");
        h.set("x-amz-storage-class", m.storageClass());
        h.set("x-amz-restore", restoreHeader(m));
        if (status == HttpStatus.PARTIAL_CONTENT) {
            h.set(HttpHeaders.CONTENT_RANGE, "bytes " + start + "-" + end + "/" + size);
        }
        return new ResponseEntity<>(new InputStreamResource(new BoundedInputStream(in, count)), h, status);
    }

    // @PutMapping("/{bucket}/{*key}")
    // public ResponseEntity<?> put(@PathVariable String bucket,
    //         @PathVariable String key,
    //         @RequestHeader(value = "x-amz-storage-class", defaultValue = "STANDARD") String storageClass,
    //         @RequestHeader(value = "x-amz-copy-source", required = false) String copySource,
    //         HttpServletRequest request) throws IOException {
    //     String actual = clean(key);

    //     // Handle CopyObject Operation
    //     if (copySource != null && !copySource.isBlank()) {
    //         return handleCopyObject(bucket, actual, copySource, storageClass);
    //     }

    //     storageClass = storageClass.toUpperCase(Locale.ROOT);
    //     if (!Set.of("STANDARD", "GLACIER", "DEEP_ARCHIVE").contains(storageClass)) {
    //         throw new S3Exception(400, "InvalidStorageClass", "Supported storage classes are STANDARD, GLACIER, DEEP_ARCHIVE", storageClass);
    //     }

    //     ObjectMetadataStore.ObjectHeaders objectHeaders = requestHeaders(request);
    //     Map<String, String> checksums = clientChecksums(request);

    //     // Unwrap AWS SigV4 chunked encoding if present
    //     InputStream inputStream = request.getInputStream();
    //     String contentSha256 = request.getHeader("x-amz-content-sha256");
    //     String contentEncoding = request.getHeader(HttpHeaders.CONTENT_ENCODING);
    //     boolean isChunked = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".equals(contentSha256)
    //             || (contentEncoding != null && contentEncoding.contains("aws-chunked"));

    //     if (isChunked) {
    //         inputStream = new AwsChunkedInputStream(inputStream);
    //     }

    //     long expectedLength = isChunked ? -1L : request.getContentLengthLong();

    //     NearlineStore.Stored stored = store.put(bucket, actual, inputStream, expectedLength, checksums, request.getHeader(HttpHeaders.IF_MATCH), request.getHeader(HttpHeaders.IF_NONE_MATCH));
    //     metadata.put(bucket, actual, storageClass, objectHeaders);
    //     requests.submit("ARCHIVE", bucket, actual);

    //     // Format ETag properly with escaped double-quotes
    //     String rawEtag = stored.etag().replaceAll("\"", "");
    //     ResponseEntity.BodyBuilder response = ResponseEntity.ok().eTag("\"" + rawEtag + "\"");

    //     stored.checksums().forEach(response::header);
    //     if (!stored.checksums().isEmpty()) {
    //         response.header("x-amz-checksum-type", "FULL_OBJECT");
    //     }
    //     return response.build();
    // }
@PutMapping("/{bucket}/{*key}")//put object 
    public ResponseEntity<?> put(@PathVariable String bucket,
                                @PathVariable String key,
                                @RequestHeader(value = "x-amz-storage-class", defaultValue = "STANDARD") String storageClass,
                                @RequestHeader(value = "x-amz-copy-source", required = false) String copySource,
                                HttpServletRequest request) throws IOException {
        String actual = clean(key);

        // 1. Handle CopyObject Operation
        if (copySource != null && !copySource.isBlank()) {
            return handleCopyObject(bucket, actual, copySource, storageClass);
        }

        storageClass = storageClass.toUpperCase(Locale.ROOT);
        if (!Set.of("STANDARD", "GLACIER", "DEEP_ARCHIVE").contains(storageClass)) {
            throw new S3Exception(400, "InvalidStorageClass", "Supported storage classes are STANDARD, GLACIER, DEEP_ARCHIVE", storageClass);
        }

        ObjectMetadataStore.ObjectHeaders objectHeaders = requestHeaders(request);
        Map<String, String> checksums = clientChecksums(request);

        // Unwrap AWS SigV4 chunked encoding if present
        InputStream inputStream = request.getInputStream();
        String contentSha256 = request.getHeader("x-amz-content-sha256");
        String contentEncoding = request.getHeader(HttpHeaders.CONTENT_ENCODING);
        boolean isChunked = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".equals(contentSha256)
                || (contentEncoding != null && contentEncoding.contains("aws-chunked"));

        if (isChunked) {
            inputStream = new AwsChunkedInputStream(inputStream);
        }

        long expectedLength = isChunked ? -1L : request.getContentLengthLong();

        NearlineStore.Stored stored = store.put(bucket, actual, inputStream, expectedLength, checksums, request.getHeader(HttpHeaders.IF_MATCH), request.getHeader(HttpHeaders.IF_NONE_MATCH));
        metadata.put(bucket, actual, storageClass, objectHeaders);
        requests.submit("ARCHIVE", bucket, actual);

        // Format ETag properly with escaped double-quotes
        String rawEtag = stored.etag().replaceAll("\"", "");
        ResponseEntity.BodyBuilder response = ResponseEntity.ok().eTag("\"" + rawEtag + "\"");

        stored.checksums().forEach(response::header);
        if (!stored.checksums().isEmpty()) {
            response.header("x-amz-checksum-type", "FULL_OBJECT");
        }
        return response.build();
    }
    @PostMapping(value = "/{bucket}/{*key}", params = "restore")//restore request for object
    public ResponseEntity<Void> restore(@PathVariable String bucket, @PathVariable String key, @RequestBody(required = false) String restoreRequest) {
        String actual = clean(key);
        store.existing(bucket, actual);
        ObjectMetadataStore.Metadata m = metadata.get(bucket, actual);
        if (!"STANDARD".equals(m.storageClass())) {
            metadata.restored(bucket, actual, restoreDays(restoreRequest));
        }
        requests.submit("RESTORE", bucket, actual);
        return ResponseEntity.accepted().header("x-amz-restore", restoreHeader(metadata.get(bucket, actual))).build();
    }

    @PostMapping(value = "/{bucket}", params = "delete", produces = MediaType.APPLICATION_XML_VALUE)// multi delete objects in bucket
    public String multiDelete(@PathVariable String bucket, @RequestBody String requestXml) throws IOException {
        // Parse key tags from XML body (<Key>sample.txt</Key>)
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("<Key>(.*?)</Key>").matcher(requestXml);
        StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");

        while (m.find()) {
            String key = m.group(1);
            try {
                Path p = store.object(bucket, key);
                Files.deleteIfExists(p);
                xml.append("<Deleted><Key>").append(xml(key)).append("</Key></Deleted>");
            } catch (Exception e) {
                xml.append("<Error><Key>").append(xml(key)).append("</Key><Code>InternalError</Code></Error>");
            }
        }
        return xml.append("</DeleteResult>").toString();
    }

    // private ResponseEntity<String> handleCopyObject(String destBucket, String destKey, String copySource, String storageClass) throws IOException {
    //     String cleanSource = copySource.startsWith("/") ? copySource.substring(1) : copySource;
    //     String[] parts = cleanSource.split("/", 2);
    //     if (parts.length < 2) {
    //         throw new S3Exception(400, "InvalidRequest", "Invalid x-amz-copy-source header", copySource);
    //     }
    //     String srcBucket = parts[0];
    //     String srcKey = parts[1];

    //     Path srcPath = store.existing(srcBucket, srcKey);
    //     ObjectMetadataStore.Metadata srcMeta = metadata.get(srcBucket, srcKey);

    //     try (InputStream in = Files.newInputStream(srcPath)) {
    //         NearlineStore.Stored stored = store.put(destBucket, destKey, in, Files.size(srcPath), Map.of(), null, null);
    //         metadata.put(destBucket, destKey, storageClass, srcMeta.headers());
    //     }

    //     String xmlResponse = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CopyObjectResult><LastModified>"
    //             + DateTimeFormatter.ISO_INSTANT.format(FileTime.fromMillis(System.currentTimeMillis()).toInstant())
    //             + "</LastModified><ETag>\"" + srcKey.hashCode() + "\"</ETag></CopyObjectResult>";

    //     return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xmlResponse);
    // }
private ResponseEntity<String> handleCopyObject(String destBucket, String destKey, String copySource, String storageClass) throws IOException {
        // 1. URL decode the header (handles %20 spaces and encoded path characters)
        String decodedSource = java.net.URLDecoder.decode(copySource, java.nio.charset.StandardCharsets.UTF_8);
        String cleanSource = decodedSource.startsWith("/") ? decodedSource.substring(1) : decodedSource;

        String[] parts = cleanSource.split("/", 2);
        if (parts.length < 2) {
            throw new S3Exception(400, "InvalidRequest", "Invalid x-amz-copy-source header", copySource);
        }
        String srcBucket = parts[0];
        String srcKey = parts[1];

        Path srcPath = store.existing(srcBucket, srcKey);
        ObjectMetadataStore.Metadata srcMeta = metadata.get(srcBucket, srcKey);

        NearlineStore.Stored stored;
        try (InputStream in = Files.newInputStream(srcPath)) {
            stored = store.put(destBucket, destKey, in, Files.size(srcPath), Map.of(), null, null);
            metadata.put(destBucket, destKey, storageClass, srcMeta.headers());
        }

        // 2. Format proper ETag with escaped quotes and standard S3 XML namespace
        String formattedEtag = "\"" + stored.etag().replaceAll("\"", "") + "\"";
        String isoTimestamp = DateTimeFormatter.ISO_INSTANT.format(FileTime.fromMillis(System.currentTimeMillis()).toInstant());

        String xmlResponse = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + "<LastModified>" + isoTimestamp + "</LastModified>"
                + "<ETag>" + formattedEtag + "</ETag>"
                + "</CopyObjectResult>";

        return ResponseEntity.ok().contentType(MediaType.APPLICATION_XML).body(xmlResponse);
    }
    private ResponseEntity.BodyBuilder headers(Path p, ObjectMetadataStore.Metadata m) throws IOException {
        HttpHeaders h = new HttpHeaders();
        applyObjectHeaders(h, m);
        ResponseEntity.BodyBuilder builder = ResponseEntity.ok().contentLength(Files.size(p)).lastModified(Files.getLastModifiedTime(p).toMillis()).header(HttpHeaders.ACCEPT_RANGES, "bytes").header("x-amz-storage-class", m.storageClass()).header("x-amz-restore", restoreHeader(m));
        h.forEach((name, values) -> builder.header(name, values.toArray(String[]::new)));
        return builder;
    }

    private static String restoreHeader(ObjectMetadataStore.Metadata m) {
        return m.restoreExpiry() == null ? "ongoing-request=\"false\"" : "ongoing-request=\"false\", expiry-date=\"" + m.restoreExpiry() + "\"";
    }

    private static int restoreDays(String body) {
        if (body == null) {
            return 7;
        }
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("<Days>\\s*(\\d+)\\s*</Days>").matcher(body);
        return m.find() ? Integer.parseInt(m.group(1)) : 7;
    }

    private static String clean(String key) {
        return key.startsWith("/") ? key.substring(1) : key;
    }

    private static MediaType contentType(String value) {
        try {
            return value == null || value.isBlank() ? MediaType.APPLICATION_OCTET_STREAM : MediaType.parseMediaType(value);
        } catch (InvalidMediaTypeException e) {
            throw new S3Exception(400, "InvalidRequest", "Content-Type is not valid", value);
        }
    }

    private static ObjectMetadataStore.ObjectHeaders requestHeaders(HttpServletRequest request) {
        Map<String, String> userMetadata = new TreeMap<>();
        Enumeration<String> names = request.getHeaderNames();
        while (names != null && names.hasMoreElements()) {
            String name = names.nextElement();
            if (name.toLowerCase(Locale.ROOT).startsWith("x-amz-meta-")) {
                userMetadata.put(name.toLowerCase(Locale.ROOT), request.getHeader(name));
            }
        }
        return new ObjectMetadataStore.ObjectHeaders(contentType(request.getContentType()).toString(), request.getHeader(HttpHeaders.CACHE_CONTROL), request.getHeader(HttpHeaders.CONTENT_DISPOSITION), request.getHeader(HttpHeaders.CONTENT_ENCODING), request.getHeader(HttpHeaders.CONTENT_LANGUAGE), request.getHeader(HttpHeaders.EXPIRES), userMetadata);
    }

    private static Map<String, String> clientChecksums(HttpServletRequest request) {
        Map<String, String> checksums = new LinkedHashMap<>();
        for (String name : List.of("content-md5", "x-amz-checksum-md5", "x-amz-checksum-crc32", "x-amz-checksum-crc32c", "x-amz-checksum-sha1", "x-amz-checksum-sha256", "x-amz-checksum-sha512")) {
            String value = request.getHeader(name);
            if (value != null && !value.isBlank()) {
                checksums.put(name, value);
            }
        }
        return checksums;
    }

    private static void applyObjectHeaders(HttpHeaders target, ObjectMetadataStore.Metadata metadata) {
        ObjectMetadataStore.ObjectHeaders h = metadata.headers();
        target.setContentType(contentType(h.contentType()));
        set(target, HttpHeaders.CACHE_CONTROL, h.cacheControl());
        set(target, HttpHeaders.CONTENT_DISPOSITION, h.contentDisposition());
        set(target, HttpHeaders.CONTENT_ENCODING, h.contentEncoding());
        set(target, HttpHeaders.CONTENT_LANGUAGE, h.contentLanguage());
        set(target, HttpHeaders.EXPIRES, h.expires());
        h.userMetadata().forEach(target::set);
    }

    private static void set(HttpHeaders headers, String name, String value) {
        if (value != null && !value.isBlank()) {
            headers.set(name, value);
        }
    }

    private static long[] parseRange(String r, long size) {
        try {
            if (!r.startsWith("bytes=")) {
                throw new Exception();
            }
            String[] v = r.substring(6).split("-", 2);
            long a = v[0].isEmpty() ? 0 : Long.parseLong(v[0]);
            long b = v[1].isEmpty() ? size - 1 : Long.parseLong(v[1]);
            if (a < 0 || b < a || a >= size) {
                throw new Exception();
            }
            return new long[]{a, Math.min(b, size - 1)};
        } catch (Exception e) {
            throw new S3Exception(416, "InvalidRange", "The requested range is not satisfiable", r);
        }
    }

    private static String time(FileTime t) {
        return DateTimeFormatter.ISO_INSTANT.format(t.toInstant());
    }

    private static String xml(String s) {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private static class BoundedInputStream extends FilterInputStream {

        long remaining;

        BoundedInputStream(InputStream in, long remaining) {
            super(in);
            this.remaining = remaining;
        }

        public int read() throws IOException {
            if (remaining == 0) {
                return -1;
            }
            int n = super.read();
            if (n >= 0) {
                remaining--;
            }
            return n;
        }

        public int read(byte[] b, int o, int l) throws IOException {
            if (remaining == 0) {
                return -1;
            }
            int n = super.read(b, o, (int) Math.min(l, remaining));
            if (n > 0) {
                remaining -= n;
            }
            return n;
        }
    }

    private static class AwsChunkedInputStream extends FilterInputStream {

        private long currentChunkRemaining = 0;
        private boolean isEof = false;

        AwsChunkedInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            byte[] b = new byte[1];
            int n = read(b, 0, 1);
            return n == -1 ? -1 : (b[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (isEof) {
                return -1;
            }

            if (currentChunkRemaining == 0) {
                readNextChunkHeader();
                if (isEof) {
                    return -1;
                }
            }

            int bytesToRead = (int) Math.min(len, currentChunkRemaining);
            int bytesRead = super.read(b, off, bytesToRead);

            if (bytesRead > 0) {
                currentChunkRemaining -= bytesRead;
                if (currentChunkRemaining == 0) {
                    readCRLF();
                }
            }

            return bytesRead;
        }

        private void readNextChunkHeader() throws IOException {
            StringBuilder header = new StringBuilder();
            int ch;
            while ((ch = super.read()) != -1) {
                if (ch == '\r') {
                    super.read(); // consume '\n'
                    break;
                }
                header.append((char) ch);
            }

            String headerLine = header.toString().trim();
            if (headerLine.isEmpty()) {
                isEof = true;
                return;
            }

            String hexSize = headerLine.split(";")[0].trim();
            try {
                currentChunkRemaining = Long.parseLong(hexSize, 16);
                if (currentChunkRemaining == 0) {
                    isEof = true;
                    readCRLF();
                }
            } catch (NumberFormatException e) {
                throw new IOException("Malformed aws-chunked size header: " + hexSize, e);
            }
        }

        private void readCRLF() throws IOException {
            int r = super.read();
            if (r == '\r') {
                super.read(); // consume '\n'
            }
        }
    }
}
