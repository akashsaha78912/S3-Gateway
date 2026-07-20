package com.mediator.s3gateway;

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

import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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

    @GetMapping(value = "/", produces = MediaType.APPLICATION_XML_VALUE)
    public String listBuckets() {
        StringBuilder x = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Buckets>");
        for (String b : store.buckets()) {
            x.append("<Bucket><Name>").append(xml(b)).append("</Name></Bucket>");

        }
        return x.append("</Buckets></ListAllMyBucketsResult>").toString();
    }

    @PutMapping("/{bucket}")
    public ResponseEntity<Void> createBucket(@PathVariable String bucket) throws IOException {
        store.createBucket(bucket);
        return ResponseEntity.ok().header(HttpHeaders.LOCATION, "/" + bucket).build();
    }

    @GetMapping(value = "/{bucket}", params = "list-type=2", produces = MediaType.APPLICATION_XML_VALUE)
    public String list(@PathVariable String bucket, @RequestParam(required = false) String prefix, @RequestParam(required = false) String delimiter, @RequestParam(defaultValue = "1000") int maxKeys, @RequestParam(required = false, name = "start-after") String after, @RequestParam(required = false, name = "continuation-token") String token) throws IOException {
        store.category(bucket);
        List<NearlineStore.Entry> all = store.list(bucket, prefix).stream().filter(v -> after == null || v.key().compareTo(after) > 0).toList();
        int start = token == null ? 0 : Integer.parseInt(new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8));
        int limit = Math.max(1, Math.min(maxKeys, 1000));
        List<NearlineStore.Entry> items = all.subList(Math.min(start, all.size()), Math.min(start + limit, all.size()));
        boolean truncated = start + items.size() < all.size();
        StringBuilder x = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>").append(xml(bucket)).append("</Name><Prefix>").append(xml(prefix)).append("</Prefix><KeyCount>").append(items.size()).append("</KeyCount><MaxKeys>").append(limit).append("</MaxKeys><IsTruncated>").append(truncated).append("</IsTruncated>");
        for (var e : items) {
            x.append("<Contents><Key>").append(xml(e.key())).append("</Key><LastModified>").append(time(e.lastModified())).append("</LastModified><ETag>\"\"</ETag><Size>").append(e.length()).append("</Size><StorageClass>").append(metadata.get(bucket, e.key()).storageClass()).append("</StorageClass></Contents>");
        }
        if (truncated) {
            x.append("<NextContinuationToken>").append(Base64.getUrlEncoder().encodeToString(Integer.toString(start + items.size()).getBytes(StandardCharsets.UTF_8))).append("</NextContinuationToken>");

        }
        return x.append("</ListBucketResult>").toString();
    }

    @RequestMapping(value = "/{bucket}/{*key}", method = RequestMethod.HEAD)
    public ResponseEntity<Void> head(@PathVariable String bucket, @PathVariable String key) throws IOException {
        String actual = clean(key);
        Path p = store.existing(bucket, actual);
        return headers(p, metadata.get(bucket, actual)).build();
    }

    @GetMapping("/{bucket}/{*key}")
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

    @PutMapping("/{bucket}/{*key}")
    public ResponseEntity<Void> put(@PathVariable String bucket, @PathVariable String key, @RequestHeader(value = "x-amz-storage-class", defaultValue = "STANDARD") String storageClass, HttpServletRequest request) throws IOException {
        String actual = clean(key);
        storageClass = storageClass.toUpperCase(Locale.ROOT);
        if (!Set.of("STANDARD", "GLACIER", "DEEP_ARCHIVE").contains(storageClass)) {
            throw new S3Exception(400, "InvalidStorageClass", "Supported storage classes are STANDARD, GLACIER, DEEP_ARCHIVE", storageClass);

        }
        ObjectMetadataStore.ObjectHeaders objectHeaders = requestHeaders(request);
        Map<String, String> checksums = clientChecksums(request);
        NearlineStore.Stored stored = store.put(bucket, actual, request.getInputStream(), request.getContentLengthLong(), checksums, request.getHeader(HttpHeaders.IF_MATCH), request.getHeader(HttpHeaders.IF_NONE_MATCH));
        metadata.put(bucket, actual, storageClass, objectHeaders);
        requests.submit("ARCHIVE", bucket, actual);
        ResponseEntity.BodyBuilder response = ResponseEntity.ok().eTag("\"" + stored.etag() + "\"");
        stored.checksums().forEach(response::header);
        if (!stored.checksums().isEmpty()) {
            response.header("x-amz-checksum-type", "FULL_OBJECT");

        }
        return response.build();
    }

    @PostMapping(value = "/{bucket}/{*key}", params = "restore")
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
}
