# S3 Nearline Gateway (standalone)

This is a focused, local-only S3-compatible gateway scaffold for the Mediator integration phase. It supports only the first-phase operations: configured-bucket listing, `ListObjectsV2`, `HEAD`, `GET` (including byte ranges), `PUT`, and `RestoreObject` (`POST ?restore`).

## Invariant

`PUT` streams only to `gateway.nearline-root`. The upload is written to a sibling staging file and atomically renamed into the configured Nearline Disk path. This gateway has no code path that writes ALTO, tape, cloud, or another storage tier.

## Run

```powershell
mvn spring-boot:run
```

The default endpoint is `http://localhost:9000`; edit `src/main/resources/application.yml` to set the NLD path and fixed bucket-to-category aliases. Authentication, ACLs, bucket deletion, multipart, and HTTPS termination are intentionally deferred.

## Bucket and category model

An S3 bucket name is only a configured alias for a Mediator category. The gateway stores objects below the category directory, not below the alias:

```text
bucket alias: sports-archive
category:     SPORTS
object key:   demo/sample.mov
NLD path:     C:/NLD/SPORTS/demo/sample.mov
```

Fixed aliases are configured under `gateway.buckets`. `CreateBucket` adds dynamic aliases to `<nearline-root>/.gateway-buckets.properties` and creates the corresponding category directory. Filesystem directories are never discovered as buckets. If configured aliases map to the same category, they expose the same NLD objects and metadata.

Dynamic categlory names are derived by uppercasing the bucket name and replacing `-` and `.` with `_`. For example, `news-archive` becomes category `NEWS_ARCHIVE` and stores objects below `C:/NLD/NEWS_ARCHIVE`.

## Smoke test

```powershell
curl.exe -X PUT --data-binary "@sample.mov" http://localhost:9000/sports-archive/demo/sample.mov
curl.exe -I http://localhost:9000/sports-archive/demo/sample.mov
curl.exe http://localhost:9000/sports-archive/demo/sample.mov --output downloaded.mov
curl.exe "http://localhost:9000/sports-archive?list-type=2&prefix=demo/"
```

Every successful PUT returns the normal ETag plus the S3 diagnostic headers `x-amz-request-id` and `x-amz-id-2`. The file is already durable in NLD at that point; a background task records the internal async `ARCHIVE` handoff in `<nearline-root>/.gateway-requests`. Replace the marked `MediatorManager.submit(request)` seam in `RequestRegistry` when attaching the existing mediator.

`POST /{bucket}/{key}?restore` returns `200` when the local file is available. When it is not local, it returns `202` and creates a `RESTORE` handoff record. Until the mediator restores the data to NLD, `GET` remains `NoSuchKey`; it never blocks or reads an offline tier.

## Notes for AWS clients

The request shapes and XML/error conventions are S3-style, but SigV4 validation is intentionally bypassed for this development phase. Configure a path-style endpoint and supply any credentials your SDK requires syntactically. Add a real `S3AuthProvider` before exposing this beyond a trusted test network.
