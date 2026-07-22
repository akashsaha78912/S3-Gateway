# S3 Nearline Gateway Architecture

> Current-code baseline: 22 July 2026. This document describes the source under
> `src/main`, including partial endpoints and known compatibility gaps. A route
> being present does not by itself mean that it is fully AWS S3 compatible.

## 1. Purpose and responsibility

This application is an S3-compatible HTTP gateway for local Nearline Disk (NLD) storage.

Its primary responsibility is:

```text
S3 client request
    → validate the S3 request
    → resolve the S3 bucket name to a Mediator category
    → read or write the object in local NLD
    → return an S3-shaped HTTP response
```

Every object upload lands on NLD first. The gateway contains no implementation that writes directly to ALTO, tape, cloud storage, or another Mediator-managed tier.

The current Mediator integration is only a file-backed seam. `RequestRegistry` records an `ARCHIVE` or `RESTORE` request, but it does not call the real Manager, Scheduler, Data Mover, or tape system.

## 2. Technology

| Item | Value |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3.3.5 |
| HTTP framework | Spring MVC / embedded Apache Tomcat |
| Build | Maven |
| Configured transport | HTTPS/TLS using the bundled PKCS12 keystore |
| Configured port | `443` |
| Java fallback NLD root | `./data/nld` |
| Configured NLD root | `C:/NLD` |
| Metadata format | Java `.properties` files |
| Tests | JUnit 5 through Spring Boot Test |

Build and run:

```powershell
mvn test
mvn spring-boot:run
```

## 3. Source layout

```text
src/main/java/com/mediator/s3gateway/
├── S3GatewayApplication.java
├── config/
│   └── GatewayProperties.java
├── exception/
│   └── S3Exception.java
├── integration/
│   └── RequestRegistry.java
├── storage/
│   ├── NearlineStore.java
│   ├── ObjectMetadataStore.java
│   └── PathUtils.java
└── web/
    ├── AwsChunkedInputStream.java
    ├── RequestIdFilter.java
    ├── S3Controller.java
    └── S3ErrorHandler.java
```

Tests mirror the relevant production packages:

```text
src/test/java/com/mediator/s3gateway/
├── storage/
│   ├── NearlineStoreTest.java
│   └── ObjectMetadataStoreTest.java
└── web/
    └── RequestIdFilterTest.java
```

## 4. Component dependency flow

```text
S3GatewayApplication
    │
    ├── GatewayProperties
    │
    ├── RequestIdFilter
    │       │
    │       ▼
    ├── S3Controller
    │       ├── NearlineStore
    │       │       └── GatewayProperties
    │       ├── ObjectMetadataStore
    │       │       ├── GatewayProperties
    │       │       └── NearlineStore
    │       └── RequestRegistry
    │               ├── GatewayProperties
    │               └── NearlineStore
    │
    └── S3ErrorHandler
            └── S3Exception
```

## 5. File-by-file responsibilities

### `S3GatewayApplication.java`

Package:

```text
com.mediator.s3gateway
```

Responsibilities:

- Main application entry point.
- Starts Spring Boot and the embedded HTTP server.
- Enables configuration-property scanning.
- Its root package causes Spring to discover all subpackages.

Main flow:

```text
main()
    → SpringApplication.run(...)
    → create Spring components
    → bind application.yml
    → start embedded Tomcat with HTTPS on configured port 443
```

### `config/GatewayProperties.java`

Responsibilities:

- Binds configuration under the `gateway` prefix.
- Provides the configured NLD root.
- Provides fixed bucket-to-category mappings.
- Provides the number of asynchronous request-record workers.

Java defaults are `./data/nld` for the NLD root and `2` workers. The current
`application.yml` overrides the NLD default with `C:/NLD`. The worker count
creates a fixed pool of ordinary Java threads for background request-record and
future Mediator-handoff work; it does not control Tomcat HTTP request capacity.

Configuration model:

```yaml
gateway:
  nearline-root: C:/NLD
  buckets:
    sports-archive: SPORTS
    deep-archive: DEEP_ARCHIVE
  async:
    workers: 2
```

### `src/main/resources/application.yml` and `keystore.p12`

The active configuration starts embedded Tomcat on HTTPS port `443` and loads a
PKCS12 keystore from the classpath. It also sets the application name, NLD root,
fixed bucket aliases, worker count, and package logging level.

The keystore password is currently present directly in YAML. That is acceptable
only for local development; production should inject the secret from an
environment variable or secret manager and use a properly managed certificate.
HTTPS encrypts transport but does not replace S3 authentication; SigV4
validation remains absent.

### `web/RequestIdFilter.java`

Responsibilities:

- Runs once for every HTTP request.
- Generates the S3 request identifier.
- Generates the extended diagnostic identifier.
- Adds both identifiers to every response.
- Makes both values available to the error handler.

Generated response headers:

```http
x-amz-request-id: <32 lowercase hexadecimal characters>
x-amz-id-2: <Base64-encoded 32 random bytes>
```

Request attributes:

```text
requestId
extendedRequestId
```

### `web/S3Controller.java`

Responsibilities:

- Defines every implemented HTTP endpoint.
- Parses S3 request paths, query parameters, and headers.
- Validates storage-class and media-type values.
- Delegates physical object I/O to `NearlineStore`.
- Delegates metadata persistence to `ObjectMetadataStore`.
- Records archive and restore handoffs through `RequestRegistry`.
- Builds success XML, headers, and streaming responses.

Implemented routes:

| Method | Route | Operation |
|---|---|---|
| `GET` | `/` | ListBuckets |
| `PUT` | `/{bucket}` | CreateBucket |
| `DELETE` | `/{bucket}` | DeleteBucket rejection stub (`403 AccessDenied`) |
| `GET` | `/{bucket}?list-type=2` | ListObjectsV2 |
| `HEAD` | `/{bucket}/{key}` | HeadObject |
| `GET` | `/{bucket}/{key}` | GetObject |
| `PUT` | `/{bucket}/{key}` | PutObject, or partial CopyObject when `x-amz-copy-source` is present |
| `DELETE` | `/{bucket}/{key}` | DeleteObject rejection stub (`501 NotImplemented`) |
| `POST` | `/{bucket}/{key}?restore` | RestoreObject |
| `POST` | `/{bucket}?delete` | Partial Multi-Object Delete |

Important private helpers:

| Helper | Purpose |
|---|---|
| `requestHeaders()` | Captures standard and `x-amz-meta-*` object metadata |
| `clientChecksums()` | Captures supported client checksum headers |
| `applyObjectHeaders()` | Replays stored metadata on GET and HEAD |
| `parseRange()` | Parses a single HTTP byte range |
| `restoreDays()` | Reads `<Days>` from restore XML |
| `restoreHeader()` | Produces `x-amz-restore` |
| `BoundedInputStream` | Prevents a ranged response from reading past its requested length |
| inner `AwsChunkedInputStream` | Removes AWS streaming chunk framing; does not verify signatures |
| `handleCopyObject()` | Performs the current local NLD-to-NLD copy implementation |
| `xml()` | Escapes listing values before placing them in XML |

The controller contains an inner `AwsChunkedInputStream`, while
`web/AwsChunkedInputStream.java` contains another implementation. The controller
uses the inner class; the standalone class is currently duplicate/unreferenced.

### `storage/NearlineStore.java`

Responsibilities:

- Resolves bucket aliases to category names.
- Loads and persists dynamically created bucket mappings.
- Constructs safe NLD object paths.
- Rejects keys that escape the category directory.
- Creates parent directories for nested keys.
- Streams uploads through staging files.
- Calculates MD5 ETags and additional checksums.
- Validates `Content-Length`.
- Validates conditional PUT headers.
- Atomically replaces completed objects.
- Lists objects from category directories.

Important records:

```java
Stored(
    Path path,
    long length,
    String etag,
    FileTime lastModified,
    Map<String, String> checksums
)
```

```java
Entry(
    String key,
    long length,
    FileTime lastModified
)
```

### `storage/ObjectMetadataStore.java`

Responsibilities:

- Stores S3 object metadata separately from object bytes.
- Scopes metadata by the resolved category rather than the bucket alias.
- Persists content type and other standard HTTP metadata.
- Persists arbitrary `x-amz-meta-*` metadata.
- Enforces the 2 KiB user-metadata limit.
- Stores storage class.
- Stores the current restore-expiry placeholder.

Important records:

```java
ObjectHeaders(
    String contentType,
    String cacheControl,
    String contentDisposition,
    String contentEncoding,
    String contentLanguage,
    String expires,
    Map<String, String> userMetadata
)
```

```java
Metadata(
    String storageClass,
    String restoreExpiry,
    ObjectHeaders headers
)
```

### `storage/PathUtils.java`

Responsibilities:

- Replaces Windows-illegal filename characters in an object key with stable
  percent-style text (`:`, `*`, `?`, `"`, `<`, `>`, and `|`).
- Allows those keys to be represented as local Windows filenames.

This is filename sanitization, not URL decoding and not a fully reversible
encoding for every possible S3 key. `NearlineStore` still performs normalized
path-containment checks after sanitization.

### `web/AwsChunkedInputStream.java`

Responsibilities:

- Parses AWS streaming chunk headers and exposes only payload bytes.
- Rejects malformed chunk framing with an `IOException`.

Current status: the controller contains a duplicate private class with the same
purpose and instantiates that private class. The standalone source file is not
currently referenced.

### `integration/RequestRegistry.java`

Responsibilities:

- Provides the current integration boundary for Mediator.
- Generates internal UUIDs for archive and restore handoffs.
- Stores request records below `.gateway-requests`.
- Changes local status from `ACCEPTED` to `SUBMITTED_TO_MANAGER`.
- Provides a helper for finding an existing restore request.

This class does not contact the real Mediator.

The real integration belongs at:

```java
/* MediatorManager.submit(request) belongs here. */
```

### `exception/S3Exception.java`

Responsibilities:

- Represents a known S3 protocol failure.
- Carries:
  - HTTP status
  - S3 error code
  - public error message
  - affected resource

Example:

```java
throw new S3Exception(
    404,
    "NoSuchBucket",
    "The specified bucket does not exist",
    bucket
);
```

### `web/S3ErrorHandler.java`

Responsibilities:

- Converts `S3Exception` into S3 XML.
- Converts unexpected exceptions into `InternalError`.
- Escapes XML values.
- Reuses identifiers created by `RequestIdFilter`.

Error response:

```http
HTTP/1.1 4xx or 5xx
Content-Type: application/xml
x-amz-request-id: ...
x-amz-id-2: ...
```

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist</Message>
  <Resource>demo/missing.txt</Resource>
  <RequestId>...</RequestId>
  <HostId>...</HostId>
</Error>
```

## 6. Bucket and category architecture

The client sees an S3 bucket name, but NLD uses a category directory:

```text
S3 bucket alias → Mediator category → NLD directory
```

Fixed example:

```text
sports-archive → SPORTS → C:\NLD\SPORTS
```

Request:

```http
PUT /sports-archive/demo/sample.txt
```

Physical object:

```text
C:\NLD\SPORTS\demo\sample.txt
```

### Fixed bucket mappings

Defined in:

```text
src/main/resources/application.yml
```

Fixed mappings take precedence over dynamically created mappings.

### Dynamic bucket mappings

`CreateBucket` derives a category by:

1. Converting the bucket name to uppercase.
2. Replacing `-` with `_`.
3. Replacing `.` with `_`.

Example:

```text
news-archive → NEWS_ARCHIVE
```

Dynamic mappings are stored in:

```text
C:\NLD\.gateway-buckets.properties
```

The mapping is loaded when `NearlineStore` is constructed, so it survives application restarts.

Arbitrary filesystem directories are not treated as buckets.

## 7. NLD disk layout

```text
C:\NLD\
├── SPORTS\
│   └── demo\
│       └── sample.txt
├── DEEP_ARCHIVE\
│   └── ...
├── NEWS_ARCHIVE\
│   └── ...
├── .gateway-buckets.properties
├── .gateway-metadata\
│   └── <Base64 category>\
│       └── <Base64 object key>.properties
└── .gateway-requests\
    └── <UUID>.properties
```

### Object content

Pattern:

```text
<nearline-root>\<category>\<object-key>
```

### Object metadata

Pattern:

```text
<nearline-root>\.gateway-metadata\
    <Base64URL(category)>\
    <Base64URL(object-key)>.properties
```

Example:

```text
Category: SPORTS
Key:      demo/sample.txt
```

Metadata values can include:

```properties
storageClass=STANDARD
contentType=text/plain
cacheControl=max-age\=3600
```

User metadata header names are encoded before being used as property names.

### Internal request records

Pattern:

```text
<nearline-root>\.gateway-requests\<UUID>.properties
```

Example:

```properties
id=<UUID>
operation=ARCHIVE
category=SPORTS
key=demo/sample.txt
status=SUBMITTED_TO_MANAGER
created=<ISO timestamp>
```

These records are internal and their IDs are not exposed as S3 version IDs or public response headers.

## 8. Request lifecycle

Every request follows:

```text
Client
  → RequestIdFilter
  → S3Controller
  → storage/integration component
  → success response
```

Failure follows:

```text
storage/controller
  → throw S3Exception
  → S3ErrorHandler
  → S3 XML response
```

## 9. PutObject flow

Request:

```http
PUT /sports-archive/year/2026/video.mp4
Content-Type: video/mp4
x-amz-storage-class: DEEP_ARCHIVE

<object bytes>
```

Execution:

```text
RequestIdFilter
  → S3Controller.put()
  → validate storage class
  → capture standard/user metadata
  → capture checksum headers
  → unwrap aws-chunked framing when request headers select it
  → NearlineStore.put()
      → resolve bucket to category
      → validate object key
      → evaluate If-Match / If-None-Match
      → create parent directories
      → stream to sibling staging file
      → count bytes
      → calculate MD5 and requested checksums
      → verify Content-Length
      → verify client checksums
      → atomic move to final object
  → ObjectMetadataStore.put()
  → RequestRegistry.submit("ARCHIVE")
  → return 200 with ETag/checksum headers
```

For an AWS streaming request, `expectedLength` is set to `-1`, so normal
`Content-Length` equality checking is skipped after chunk framing is removed.
Chunk signatures are not cryptographically verified; SigV4 authentication is
still unimplemented.

### Staging

Final object:

```text
C:\NLD\SPORTS\year\2026\video.mp4
```

Temporary sibling:

```text
C:\NLD\SPORTS\year\2026\
    .video.mp4.<UUID>.uploading
```

The staging file is removed if streaming, length validation, or checksum validation fails.

### Replacement

The final move uses:

```text
ATOMIC_MOVE
REPLACE_EXISTING
```

If atomic moves are unsupported by the filesystem, the code falls back to `REPLACE_EXISTING`.

### Per-object synchronization

`NearlineStore` maintains an in-process lock for each resolved object path. Conditional evaluation and replacement happen inside that lock, preventing two requests handled by the same gateway process from simultaneously passing the same write condition.

## 10. Supported PutObject metadata

Standard headers:

```text
Content-Type
Cache-Control
Content-Disposition
Content-Encoding
Content-Language
Expires
```

User metadata:

```text
x-amz-meta-*
```

User metadata names are normalized to lowercase.

The combined UTF-8 byte count of all user-metadata keys and values must not exceed 2,048 bytes. A larger value produces:

```text
400 MetadataTooLarge
```

## 11. Storage-class handling

Supported:

```text
STANDARD
GLACIER
DEEP_ARCHIVE
```

If absent:

```text
STANDARD
```

Every class lands in NLD first. The value records requested storage behavior; it does not cause this gateway to write directly to an offline tier.

Unsupported values produce:

```text
400 InvalidStorageClass
```

## 12. Checksum and ETag handling

MD5 is always calculated while streaming. For normal single-request uploads, its lowercase hexadecimal form becomes the ETag.

Supported client checksum headers:

```text
Content-MD5
x-amz-checksum-md5
x-amz-checksum-crc32
x-amz-checksum-crc32c
x-amz-checksum-sha1
x-amz-checksum-sha256
x-amz-checksum-sha512
```

Checksum flow:

```text
validate Base64 and expected digest length
  → calculate while streaming
  → constant-time comparison
  → delete staging file on mismatch
```

Errors:

```text
InvalidDigest → malformed Base64 or wrong digest length
BadDigest     → supplied checksum differs from calculated checksum
```

Verified `x-amz-checksum-*` headers are returned in the successful PUT response together with:

```http
x-amz-checksum-type: FULL_OBJECT
```

`Content-MD5` is request-only and is not echoed.

## 13. Conditional PutObject

### Create only

```http
If-None-Match: *
```

Behavior:

```text
missing object  → upload
existing object → 412 PreconditionFailed
```

### Compare and replace

```http
If-Match: "<current-etag>"
```

Behavior:

```text
matching object → replace
wrong ETag      → 412 PreconditionFailed
missing object  → 404 NoSuchKey
```

Without either header, an existing object is replaced.

## 14. CreateBucket flow

Request:

```http
PUT /news-archive
Content-Length: 0
```

Execution:

```text
S3Controller.createBucket()
  → NearlineStore.createBucket()
      → validate bucket name
      → reject existing mapping
      → derive NEWS_ARCHIVE
      → reject category collision
      → create C:\NLD\NEWS_ARCHIVE
      → atomically persist dynamic mapping
  → return 200 and Location: /news-archive
```

Errors:

```text
400 InvalidBucketName
409 BucketAlreadyOwnedByYou
409 BucketAlreadyExists
500 InternalError
```

## 15. GetObject flow

Request:

```http
GET /sports-archive/demo/sample.txt
```

Execution:

```text
S3Controller.get()
  → NearlineStore.existing()
  → open local NLD file
  → ObjectMetadataStore.get()
  → apply stored response headers
  → stream file to client
```

The gateway never fetches directly from tape or another offline tier.

### Byte ranges

Supported form:

```http
Range: bytes=0-99
```

Response:

```http
206 Partial Content
Content-Range: bytes 0-99/<total>
Content-Length: 100
Accept-Ranges: bytes
```

`BoundedInputStream` ensures the response does not stream bytes beyond the requested end position.

Invalid ranges produce:

```text
416 InvalidRange
```

## 16. HeadObject flow

Request:

```http
HEAD /sports-archive/demo/sample.txt
```

Execution:

```text
S3Controller.head()
  → NearlineStore.existing()
  → ObjectMetadataStore.get()
  → return object headers with no body
```

Returned information includes:

```text
Content-Length
Last-Modified
Content-Type
Accept-Ranges
x-amz-storage-class
x-amz-restore
stored cache/content/user metadata
```

## 17. ListBuckets flow

Request:

```http
GET /
```

`NearlineStore.buckets()` combines:

- Fixed mappings in `application.yml`.
- Dynamic mappings in `.gateway-buckets.properties`.

Names are sorted and returned as S3 XML.

## 18. ListObjectsV2 flow

Request:

```http
GET /sports-archive?list-type=2&prefix=demo/&delimiter=/&maxKeys=100
```

Processing:

```text
resolve bucket/category
  → walk the category directory
  → retain regular files
  → apply prefix
  → sort by key
  → apply start-after
  → group deeper keys into CommonPrefixes when delimiter is present
  → apply numeric-offset continuation token to direct object entries
  → read storage class metadata
  → generate XML
```

Currently accepted query values:

```text
list-type=2
prefix
delimiter
maxKeys
start-after
continuation-token
```

Current parameter and compatibility details:

- The Java parameter is named `maxKeys`, so the accepted query spelling is
  currently `maxKeys`; AWS normally sends `max-keys`.
- `maxKeys` is clamped to 1 through 1,000. AWS permits zero, but this
  implementation changes zero to one.
- `delimiter` grouping emits unique, sorted `CommonPrefixes`.
- Pagination applies only to direct `Contents` entries. `CommonPrefixes` are not
  paginated, yet all of them are added to `KeyCount`; `KeyCount` can therefore
  exceed `MaxKeys`.
- A continuation token is only a Base64 URL-encoded numeric list index. It is
  not signed, tied to request parameters, or stable if files change between
  pages.
- Invalid or negative tokens are not converted into a specific S3 client error
  and can become `500 InternalError`.
- Listing ETags are currently Java `key.hashCode()` values, not the MD5 ETags
  produced by PutObject.

## 19. RestoreObject flow

Request:

```http
POST /sports-archive/demo/sample.txt?restore
Content-Type: application/xml

<RestoreRequest>
  <Days>7</Days>
</RestoreRequest>
```

Execution:

```text
S3Controller.restore()
  → require local NLD file
  → read storage class
  → write restoreExpiry for non-STANDARD metadata
  → RequestRegistry.submit("RESTORE")
  → return 202
```

The restore record is a placeholder. The current code does not:

- Submit to a real Mediator Manager.
- Leave restore state in a real `IN_PROGRESS` state.
- Restore an object absent from NLD.
- Poll a real external request.

## 20. Partial CopyObject and deletion routes

### Partial CopyObject behavior

When `PUT /{bucket}/{key}` contains `x-amz-copy-source`, the controller:

```text
parse source bucket/key
  → require the source NLD file
  → stream source bytes through NearlineStore.put()
  → copy source metadata while using the requested destination storage class
  → return CopyObjectResult XML
```

This path is partial: its response ETag is derived from `srcKey.hashCode()`
rather than content MD5, it does not run normal destination storage-class
validation/normalization, it does not submit an `ARCHIVE` request, and it does
not implement all S3 copy directives or URL-decoding rules.

### Current deletion behavior

- `DELETE /{bucket}` never deletes and returns a handcrafted `403 AccessDenied`
  XML response.
- `DELETE /{bucket}/{key}` never deletes and returns a handcrafted
  `501 NotImplemented` XML response.
- Those two handcrafted bodies contain the fixed request ID
  `44442222AAAA2222`, which can disagree with the actual request IDs added by
  `RequestIdFilter`.
- `POST /{bucket}?delete` extracts `<Key>` values with a regular expression and
  deletes matching object files. It does not delete metadata, does not use a
  secure XML parser, and is inconsistent with the single-delete rejection.

These deletion routes are stubs/partial behavior, not a complete S3 deletion
implementation.

## 21. Error handling

Known S3 errors raised by the implementation include:

| HTTP | Code | Typical source |
|---:|---|---|
| `400` | `InvalidBucketName` | Bucket-name validation |
| `400` | `InvalidRequest` | Missing key, invalid media type, invalid conditional value |
| `400` | `InvalidURI` | Key escapes category root |
| `400` | `InvalidStorageClass` | Unsupported storage class |
| `400` | `MetadataTooLarge` | User metadata exceeds 2 KiB |
| `400` | `IncompleteBody` | Normal stream completion with length mismatch |
| `400` | `InvalidDigest` | Malformed checksum |
| `400` | `BadDigest` | Checksum mismatch |
| `404` | `NoSuchBucket` | Unknown alias |
| `404` | `NoSuchKey` | Missing local object |
| `409` | `BucketAlreadyExists` | Dynamic category collision |
| `409` | `BucketAlreadyOwnedByYou` | Existing mapping |
| `412` | `PreconditionFailed` | Conditional upload failed |
| `416` | `InvalidRange` | Unsatisfiable GET range |
| `500` | `InternalError` | Unexpected exception |

## 22. Tests and feature coverage

### `storage/NearlineStoreTest.java`

Covers:

- Category-scoped object paths.
- Multiple aliases sharing one category.
- Configured-only bucket visibility.
- Unknown buckets.
- Category and object path traversal.
- MD5 and additional checksum verification.
- Staging-file cleanup.
- Invalid and mismatched digests.
- `If-Match`.
- `If-None-Match`.
- Dynamic bucket persistence.
- Existing bucket errors.
- Dynamic category collisions.
- Invalid dynamic bucket names.

### `storage/ObjectMetadataStoreTest.java`

Covers:

- Shared metadata for aliases mapped to one category.
- Standard metadata persistence.
- User metadata persistence.
- Default content type for older/missing metadata.
- 2 KiB user-metadata limit.

### `web/RequestIdFilterTest.java`

Covers:

- `x-amz-request-id`.
- `x-amz-id-2`.
- Matching request attributes used by the error handler.

## 23. Transaction boundaries and failure behavior

### Upload success boundary

The object becomes visible only after staging completes and the final move succeeds.

Order:

```text
object bytes finalized
  → metadata written
  → archive request record written
  → HTTP success returned
```

Important consequence: if metadata or request-record persistence fails after the object move, the client can receive an error while the completed object file already exists in NLD.

### Bucket creation boundary

Order:

```text
category directory created
  → dynamic mapping persisted
```

If mapping persistence fails, the in-memory mapping is removed and a newly created empty category directory is deleted when possible.

## 24. Security and filesystem protections

Implemented protections:

- Bucket-name validation.
- Category-root normalization.
- Object path normalization.
- Rejection when an object path escapes its category root.
- Staging files prevent partial-object visibility.
- Constant-time checksum comparison.
- XML escaping in responses.
- Per-object in-process write synchronization.
- HTTPS/TLS termination in embedded Tomcat using the configured PKCS12 keystore.

Not yet implemented:

- AWS Signature Version 4.
- Authentication/authorization.
- Rate limiting.
- Storage quotas.
- A full encoding layer for every S3-valid key that Windows cannot represent.

## 25. Explicitly unsupported features

The current gateway does not fully implement:

- DeleteObject or DeleteBucket semantics. Routes exist only as rejection stubs;
  Multi-Object Delete is partial and inconsistent.
- Multipart upload.
- Complete CopyObject semantics. A partial local NLD-to-NLD copy path exists.
- Object tagging APIs.
- Server-side encryption.
- Object Lock.
- Versioning.
- ACL or bucket policy APIs.
- Lifecycle configuration.
- Presigned URL validation.
- Virtual-hosted-style bucket routing.
- Real Mediator submission.

Unsupported features must not be described as active merely because a similarly named header is received.

## 26. Extension points

### Real Mediator integration

Replace the file-backed action inside:

```text
integration/RequestRegistry.java
```

with a dedicated Manager client or port. NLD storage code should remain independent of Manager transport details.

### Persistent object catalogue

The current listing is based on files present in NLD. If Mediator removes archived files from NLD, a persistent object catalogue will be needed to distinguish:

```text
unknown object
archived but offline object
restoring object
available NLD object
```

### Restore state machine

A production restore implementation should model:

```text
NONE
  → IN_PROGRESS
  → AVAILABLE
  → EXPIRED
```

and update `x-amz-restore` based on the real Mediator state.

### Metadata durability

For stronger consistency, object finalization, metadata persistence, and handoff creation should use an explicit recoverable transaction or reconciliation process.

## 27. Operational summary

The key invariant is:

```text
The S3 gateway reads and writes only NLD.
Mediator-managed movement begins after the local NLD operation.
```

The principal upload path is:

```text
RequestIdFilter
  → S3Controller.put
  → NearlineStore.put
  → ObjectMetadataStore.put
  → RequestRegistry.submit
  → S3 response
```

The principal read path is:

```text
RequestIdFilter
  → S3Controller.get/head
  → NearlineStore.existing
  → ObjectMetadataStore.get
  → streamed S3 response
```

The principal failure path is:

```text
S3Exception
  → S3ErrorHandler
  → S3 XML error with RequestId and HostId
```
