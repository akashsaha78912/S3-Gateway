package com.mediator.s3gateway.storage;

import com.mediator.s3gateway.config.GatewayProperties;
import com.mediator.s3gateway.exception.S3Exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ObjectMetadataStoreTest {
  @TempDir Path temp;

  @Test
  void aliasesForTheSameCategoryShareMetadata() {
    GatewayProperties properties=new GatewayProperties();
    properties.setNearlineRoot(temp);
    properties.setBuckets(new LinkedHashMap<>());
    properties.getBuckets().put("archive-one","ARCHIVE");
    properties.getBuckets().put("archive-two","ARCHIVE");
    NearlineStore nearline=new NearlineStore(properties);
    ObjectMetadataStore metadata=new ObjectMetadataStore(properties,nearline);

    ObjectMetadataStore.ObjectHeaders headers=new ObjectMetadataStore.ObjectHeaders(
        "video/mp4","max-age=3600","attachment","gzip","en-US",
        "Fri, 18 Jul 2026 12:00:00 GMT",
        Map.of("x-amz-meta-customer-id","1234","x-amz-meta-source","camera"));
    metadata.put("archive-one","demo.mov","DEEP_ARCHIVE",headers);

    ObjectMetadataStore.Metadata saved=metadata.get("archive-two","demo.mov");
    assertEquals("DEEP_ARCHIVE",saved.storageClass());
    assertEquals("video/mp4",saved.contentType());
    assertEquals("max-age=3600",saved.headers().cacheControl());
    assertEquals("attachment",saved.headers().contentDisposition());
    assertEquals("gzip",saved.headers().contentEncoding());
    assertEquals("en-US",saved.headers().contentLanguage());
    assertEquals("Fri, 18 Jul 2026 12:00:00 GMT",saved.headers().expires());
    assertEquals("1234",saved.headers().userMetadata().get("x-amz-meta-customer-id"));
    assertEquals("camera",saved.headers().userMetadata().get("x-amz-meta-source"));
  }

  @Test
  void defaultsMissingContentTypeForExistingMetadata() {
    GatewayProperties properties=new GatewayProperties();
    properties.setNearlineRoot(temp);
    properties.setBuckets(new LinkedHashMap<>());
    properties.getBuckets().put("archive-one","ARCHIVE");
    NearlineStore nearline=new NearlineStore(properties);
    ObjectMetadataStore metadata=new ObjectMetadataStore(properties,nearline);

    assertEquals("application/octet-stream",metadata.get("archive-one","old-object.bin").contentType());
  }

  @Test
  void rejectsUserMetadataLargerThanTwoKiB() {
    String oversized="x".repeat(2048);

    S3Exception error=assertThrows(S3Exception.class,()->new ObjectMetadataStore.ObjectHeaders(
        "application/octet-stream",null,null,null,null,null,Map.of("x-amz-meta-key",oversized)));

    assertEquals(400,error.status());
    assertEquals("MetadataTooLarge",error.code());
  }
}
