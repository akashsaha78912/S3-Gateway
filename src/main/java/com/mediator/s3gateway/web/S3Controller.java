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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * Exposes the supported S3-compatible HTTP operations.
 *
 * <p>This class owns HTTP concerns: route selection, request headers, query
 * parameters, XML response construction and HTTP status codes. Physical NLD
 * file access is delegated to {@link NearlineStore}, object metadata is
 * delegated to {@link ObjectMetadataStore}, and archive/restore work is
 * recorded through {@link RequestRegistry}.
 */
@RestController
public class S3Controller {

    private static final Logger log = LoggerFactory.getLogger(S3Controller.class);

    private final NearlineStore store;
    private final RequestRegistry requests;
    private final ObjectMetadataStore metadata;

    /**
     * Spring injects the controller's storage collaborators through this
     * constructor.
     */
    public S3Controller(NearlineStore store, RequestRegistry requests, ObjectMetadataStore metadata) {
        this.store = store;
        this.requests = requests;
        this.metadata = metadata;
    }

    /**
     * Handles S3 ListBuckets: {@code GET /}.
     *
     * @return an AWS-style XML document containing every visible bucket
     */
    @GetMapping(value = "/", produces = MediaType.APPLICATION_XML_VALUE)
    public String listBuckets(HttpServletRequest request) {
        logRequest(request);
        log.info("Executing ListBuckets operation");

        // Build the S3 XML response from the bucket names exposed by the store.
        StringBuilder x = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Buckets>");
        for (String b : store.buckets()) {
            x.append("<Bucket><Name>").append(xml(b)).append("</Name></Bucket>");
        }
        return x.append("</Buckets></ListAllMyBucketsResult>").toString();
    }

    /**
     * Handles CreateBucket: {@code PUT /{bucket}}.
     *
     * <p>The store validates the S3 bucket name and creates its category
     * directory below the configured nearline root.
     */
    @PutMapping("/{bucket}")
    public ResponseEntity<Void> createBucket(@PathVariable String bucket, HttpServletRequest request) throws IOException {
        logRequest(request);
        log.info("Executing CreateBucket operation for bucket: {}", bucket);

        store.createBucket(bucket);
        return ResponseEntity.ok().header(HttpHeaders.LOCATION, "/" + bucket).build();
    }

    /**
     * Handles DeleteBucket requests, which are intentionally disabled.
     *
     * @return an S3 XML AccessDenied response without touching NLD data
     */
    @DeleteMapping("/{bucket}")
    public ResponseEntity<String> deleteBucket(@PathVariable String bucket, HttpServletRequest request) {
        logRequest(request);
        log.warn("Executing DeleteBucket (Disabled operation) for bucket: {}", bucket);

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

    /**
     * Handles DeleteObject requests, which are intentionally disabled.
     *
     * @return an S3 XML NotImplemented response without deleting the object
     */
    @DeleteMapping("/{bucket}/{*key}")
    public ResponseEntity<String> deleteObject(@PathVariable String bucket, @PathVariable String key, HttpServletRequest request) {
        logRequest(request);
        String actual = clean(key);
        log.warn("Executing DeleteObject (Disabled operation) for bucket: {}, key: {}", bucket, actual);

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

    /**
     * Handles ListObjectsV2:
     * {@code GET /{bucket}?list-type=2}.
     *
     * <p>Supports prefix filtering, delimiter grouping, start-after,
     * continuation tokens and a maximum page size of 1000.
     */
    @GetMapping(value = "/{bucket}", params = "list-type=2", produces = MediaType.APPLICATION_XML_VALUE)
    public String list(@PathVariable String bucket,
            @RequestParam(required = false) String prefix,
            @RequestParam(required = false) String delimiter,
            @RequestParam(defaultValue = "1000") int maxKeys,
            @RequestParam(required = false, name = "start-after") String after,
            @RequestParam(required = false, name = "continuation-token") String token,
            HttpServletRequest request) throws IOException {

        logRequest(request);
        log.info("Executing ListObjectsV2 - Bucket: {}, Prefix: '{}', Delimiter: '{}', MaxKeys: {}, StartAfter: '{}', Token: '{}'",
                bucket, prefix, delimiter, maxKeys, after, token);

        // Resolve the bucket first so an unknown bucket returns NoSuchBucket.
        store.category(bucket);
        String pfx = prefix == null ? "" : prefix;
        List<NearlineStore.Entry> all = store.list(bucket, prefix).stream()
                .filter(v -> after == null || v.key().compareTo(after) > 0)
                .toList();

        // This implementation's continuation token is a Base64-encoded list index.
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

        // Construct the AWS ListObjectsV2 XML response.
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

    /**
     * Handles HeadObject: {@code HEAD /{bucket}/{key}}.
     *
     * <p>Returns the same object headers as GET but does not return file bytes.
     */
    @RequestMapping(value = "/{bucket}/{*key}", method = RequestMethod.HEAD)
    public ResponseEntity<Void> head(@PathVariable String bucket, @PathVariable String key, HttpServletRequest request) throws IOException {
        logRequest(request);
        String actual = clean(key);
        log.info("Executing HeadObject - Bucket: {}, Key: {}", bucket, actual);

        Path p = store.existing(bucket, actual);
        return headers(p, metadata.get(bucket, actual)).build();
    }

    /**
     * Handles GetObject: {@code GET /{bucket}/{key}}.
     *
     * <p>A request without Range returns 200 and the full file. A valid
     * single byte range returns 206, Content-Range and only the selected bytes.
     */
    @GetMapping("/{bucket}/{*key}")
    public ResponseEntity<InputStreamResource> get(@PathVariable String bucket,
            @PathVariable String key,
            @RequestHeader(value = "Range", required = false) String range,
            HttpServletRequest request) throws IOException {
        logRequest(request);
        String actual = clean(key);
        log.info("Executing GetObject - Bucket: {}, Key: {}, Range Header: {}", bucket, actual, range);

        // Resolve and validate the object before opening its NLD file.
        Path p = store.existing(bucket, actual);
        long size = Files.size(p), start = 0, end = size - 1;
        HttpStatus status = HttpStatus.OK;
        if (range != null) {
            long[] parsed = parseRange(range, size);
            start = parsed[0];
            end = parsed[1];
            status = HttpStatus.PARTIAL_CONTENT;
        }
        // Start reading at the requested byte offset.
        InputStream in = Files.newInputStream(p);
        in.skipNBytes(start);
        long count = end - start + 1;
        HttpHeaders h = new HttpHeaders();
        h.setContentLength(count);
        h.setLastModified(Files.getLastModifiedTime(p).toMillis());
        ObjectMetadataStore.Metadata m = metadata.get(bucket, actual);
        applyObjectHeaders(h, m);
        h.set(HttpHeaders.ACCEPT_RANGES, "bytes");
        h.set("x-amz-storage-class", m.storageClass());
        h.set("x-amz-restore", restoreHeader(m));
        if (status == HttpStatus.PARTIAL_CONTENT) {
            h.set(HttpHeaders.CONTENT_RANGE, "bytes " + start + "-" + end + "/" + size);
        }
        // BoundedInputStream prevents a range response from reading past its end.
        return new ResponseEntity<>(new InputStreamResource(new BoundedInputStream(in, count)), h, status);
    }

    /**
     * Handles PutObject: {@code PUT /{bucket}/{key}}.
     *
     * <p>The request body always lands in the local nearline store first.
     * After the object write succeeds, this method persists its HTTP metadata,
     * records the archive request and returns the calculated ETag.
     */
    @PutMapping("/{bucket}/{*key}")
    public ResponseEntity<?> put(@PathVariable String bucket,
            @PathVariable String key,
            @RequestHeader(value = "x-amz-storage-class", defaultValue = "STANDARD") String storageClass,
            HttpServletRequest request) throws IOException {
        logRequest(request);
        String actual = clean(key);

        log.info("Executing PutObject - Bucket: {}, Key: {}, StorageClass: {}", bucket, actual, storageClass);

        storageClass = storageClass.toUpperCase(Locale.ROOT);
        if (!Set.of("STANDARD", "GLACIER", "DEEP_ARCHIVE").contains(storageClass)) {
            log.error("Invalid Storage Class specified: {}", storageClass);
            throw new S3Exception(400, "InvalidStorageClass", "Supported storage classes are STANDARD, GLACIER, DEEP_ARCHIVE", storageClass);
        }

        // Capture standard representation headers and all x-amz-meta-* values.
        ObjectMetadataStore.ObjectHeaders objectHeaders = requestHeaders(request);
        Map<String, String> checksums = clientChecksums(request);

        // Pass the ordinary servlet request stream directly to NearlineStore.
        InputStream inputStream = request.getInputStream();
        long expectedLength = request.getContentLengthLong();

        // NearlineStore validates the key/checksums and atomically publishes the file.
        NearlineStore.Stored stored = store.put(
                bucket,
                actual,
                inputStream,
                expectedLength,
                checksums,
                request.getHeader(HttpHeaders.IF_MATCH),
                request.getHeader(HttpHeaders.IF_NONE_MATCH)
        );

        // Make the completed NLD landing visible in the application log.
        log.info(
                "PUT received. Content-Length: {}, saved to: {}",
                stored.length(),
                stored.path()
        );

        // Persist metadata only after the object bytes have been saved successfully.
        metadata.put(bucket, actual, storageClass, objectHeaders);

        // Record the asynchronous archive hand-off; this does not replace NLD storage.
        requests.submit("ARCHIVE", bucket, actual);
        String rawEtag = stored.etag().replace("\"", "");

        ResponseEntity.BodyBuilder response = ResponseEntity.ok()
                .contentLength(0)
                .eTag("\"" + rawEtag + "\"");

        // Echo each checksum that NearlineStore calculated and verified.
        stored.checksums().forEach(response::header);

        if (!stored.checksums().isEmpty()) {
            response.header("x-amz-checksum-type", "FULL_OBJECT");
        }

        return response.build();
    }

    /**
     * Handles RestoreObject: {@code POST /{bucket}/{key}?restore}.
     *
     * <p>The object must already exist. Archived objects receive a temporary
     * restore expiry and every accepted request is recorded for later work.
     */
    @PostMapping(value = "/{bucket}/{*key}", params = "restore")
    public ResponseEntity<Void> restore(@PathVariable String bucket,
            @PathVariable String key,
            @RequestBody(required = false) String restoreRequest,
            HttpServletRequest request) {
        logRequest(request);
        String actual = clean(key);
        log.info("Executing RestoreObject - Bucket: {}, Key: {}, Restore Body: {}", bucket, actual, restoreRequest);

        // Fail with NoSuchKey before changing metadata or submitting work.
        store.existing(bucket, actual);
        ObjectMetadataStore.Metadata m = metadata.get(bucket, actual);
        if (!"STANDARD".equals(m.storageClass())) {
            metadata.restored(bucket, actual, restoreDays(restoreRequest));
        }
        requests.submit("RESTORE", bucket, actual);
        return ResponseEntity.accepted().header("x-amz-restore", restoreHeader(metadata.get(bucket, actual))).build();
    }

    /**
     * Handles the S3 multi-object delete request:
     * {@code POST /{bucket}?delete}.
     *
     * <p>Each Key element is processed independently and represented as either
     * Deleted or Error in the response XML.
     */
    @PostMapping(value = "/{bucket}", params = "delete", produces = MediaType.APPLICATION_XML_VALUE)
    public String multiDelete(@PathVariable String bucket, @RequestBody String requestXml, HttpServletRequest request) throws IOException {
        logRequest(request);
        log.info("Executing MultiDelete - Bucket: {}, Body XML: {}", bucket, requestXml);

        // Parse key tags from XML body (<Key>sample.txt</Key>)
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("<Key>(.*?)</Key>").matcher(requestXml);
        StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");

        while (m.find()) {
            String key = m.group(1);
            try {
                Path p = store.object(bucket, key);
                Files.deleteIfExists(p);
                xml.append("<Deleted><Key>").append(xml(key)).append("</Key></Deleted>");
                log.debug("MultiDelete: Successfully deleted object key: {}", key);
            } catch (Exception e) {
                log.error("MultiDelete: Failed to delete object key: {}", key, e);
                xml.append("<Error><Key>").append(xml(key)).append("</Key><Code>InternalError</Code></Error>");
            }
        }
        return xml.append("</DeleteResult>").toString();
    }

    /**
     * Logs the incoming method, URI, query string and request headers.
     *
     * <p>Do not use this unchanged in an environment where headers may contain
     * credentials or other secrets.
     */
    private void logRequest(HttpServletRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n================ INCOMING SDK REQUEST ================");
        sb.append("\nHTTP Method : ").append(request.getMethod());
        sb.append("\nRequest URI : ").append(request.getRequestURI());
        if (request.getQueryString() != null) {
            sb.append("\nQuery String: ").append(request.getQueryString());
        }
        sb.append("\nHeaders     :");

        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames != null && headerNames.hasMoreElements()) {
            String name = headerNames.nextElement();
            sb.append("\n  ").append(name).append(" = ").append(request.getHeader(name));
        }
        sb.append("\n======================================================");
        log.info(sb.toString());
    }

    /**
     * Builds the common successful HeadObject response headers.
     */
    private ResponseEntity.BodyBuilder headers(Path p, ObjectMetadataStore.Metadata m) throws IOException {
        HttpHeaders h = new HttpHeaders();
        applyObjectHeaders(h, m);
        ResponseEntity.BodyBuilder builder = ResponseEntity.ok().contentLength(Files.size(p)).lastModified(Files.getLastModifiedTime(p).toMillis()).header(HttpHeaders.ACCEPT_RANGES, "bytes").header("x-amz-storage-class", m.storageClass()).header("x-amz-restore", restoreHeader(m));
        h.forEach((name, values) -> builder.header(name, values.toArray(String[]::new)));
        return builder;
    }

    /**
     * Converts stored restore state into the S3 x-amz-restore header format.
     */
    private static String restoreHeader(ObjectMetadataStore.Metadata m) {
        return m.restoreExpiry() == null ? "ongoing-request=\"false\"" : "ongoing-request=\"false\", expiry-date=\"" + m.restoreExpiry() + "\"";
    }

    /**
     * Reads the requested restore duration from the XML body, defaulting to
     * seven days when the body or Days element is absent.
     */
    private static int restoreDays(String body) {
        if (body == null) {
            return 7;
        }
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("<Days>\\s*(\\d+)\\s*</Days>").matcher(body);
        return m.find() ? Integer.parseInt(m.group(1)) : 7;
    }

    /**
     * Spring's {*key} capture includes a leading slash; storage keys do not.
     */
    private static String clean(String key) {
        return key.startsWith("/") ? key.substring(1) : key;
    }

    /**
     * Parses a Content-Type value and translates invalid input into an S3 error.
     */
    private static MediaType contentType(String value) {
        try {
            return value == null || value.isBlank() ? MediaType.APPLICATION_OCTET_STREAM : MediaType.parseMediaType(value);
        } catch (InvalidMediaTypeException e) {
            throw new S3Exception(400, "InvalidRequest", "Content-Type is not valid", value);
        }
    }

    /**
     * Extracts persistable object headers from a PutObject request.
     */
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

    /**
     * Collects supported checksum headers for validation by NearlineStore.
     */
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

    /**
     * Restores persisted content and user-metadata headers onto GET/HEAD.
     */
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

    /**
     * Adds a response header only when its persisted value is meaningful.
     */
    private static void set(HttpHeaders headers, String name, String value) {
        if (value != null && !value.isBlank()) {
            headers.set(name, value);
        }
    }

    /**
     * Parses one HTTP bytes range and clamps its end to the object size.
     *
     * @return a two-element array containing the inclusive start and end
     */
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

    /**
     * Formats a file timestamp for S3 XML responses.
     */
    private static String time(FileTime t) {
        return DateTimeFormatter.ISO_INSTANT.format(t.toInstant());
    }

    /**
     * Escapes text before inserting it into an XML response.
     */
    private static String xml(String s) {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    /**
     * Limits an underlying file stream to the selected response byte count.
     */
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

}
