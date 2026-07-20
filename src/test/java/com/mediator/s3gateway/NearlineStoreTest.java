package com.mediator.s3gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NearlineStoreTest {
  @TempDir Path temp;

  @Test
  void storesObjectsUnderMappedCategoryInsteadOfBucketAlias() throws Exception {
    NearlineStore store=store("sports-archive","SPORTS");

    store.put("sports-archive","demo/sample.txt",bytes("hello"),5);

    assertTrue(Files.isRegularFile(temp.resolve("SPORTS/demo/sample.txt")));
    assertFalse(Files.exists(temp.resolve("sports-archive")));
  }

  @Test
  void aliasesForTheSameCategoryExposeTheSameObjects() throws Exception {
    GatewayProperties properties=properties();
    properties.getBuckets().put("sports-archive","SPORTS");
    properties.getBuckets().put("sports-backup","SPORTS");
    NearlineStore store=new NearlineStore(properties);

    store.put("sports-archive","video.mov",bytes("data"),4);

    assertEquals(store.existing("sports-archive","video.mov"),store.existing("sports-backup","video.mov"));
    assertEquals(1,store.list("sports-backup",null).size());
  }

  @Test
  void listsOnlyConfiguredAliases() throws Exception {
    NearlineStore store=store("sports-archive","SPORTS");
    Files.createDirectories(temp.resolve("unconfigured-directory"));

    assertEquals(java.util.Set.of("sports-archive"),store.buckets());
  }

  @Test
  void rejectsUnknownBucketEvenWhenMatchingDirectoryExists() throws Exception {
    NearlineStore store=store("sports-archive","SPORTS");
    Files.createDirectories(temp.resolve("unknown-bucket"));

    S3Exception error=assertThrows(S3Exception.class,() -> store.object("unknown-bucket","file.txt"));

    assertEquals(404,error.status());
    assertEquals("NoSuchBucket",error.code());
  }

  @Test
  void rejectsCategoryThatEscapesNearlineRoot() {
    NearlineStore store=store("sports-archive","../outside");

    assertThrows(IllegalStateException.class,() -> store.object("sports-archive","file.txt"));
  }

  @Test
  void acceptsMatchingClientMd5() throws Exception {
    NearlineStore store=store("sports-archive","SPORTS");
    byte[] content="checksum data".getBytes(StandardCharsets.UTF_8);
    String checksum=Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(content));

    store.put("sports-archive","verified.txt",new ByteArrayInputStream(content),content.length,Map.of("content-md5",checksum));

    assertTrue(Files.isRegularFile(temp.resolve("SPORTS/verified.txt")));
  }

  @Test
  void returnsVerifiedAdditionalChecksum() throws Exception {
    NearlineStore store=store("sports-archive","SPORTS");
    byte[] content="hello".getBytes(StandardCharsets.UTF_8);
    String checksum=Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(content));

    NearlineStore.Stored stored=store.put("sports-archive","sha256.txt",new ByteArrayInputStream(content),content.length,Map.of("x-amz-checksum-sha256",checksum));

    assertEquals(checksum,stored.checksums().get("x-amz-checksum-sha256"));
  }

  @Test
  void traversalUsesStandardInvalidUriError() {
    NearlineStore store=store("sports-archive","SPORTS");

    S3Exception error=assertThrows(S3Exception.class,()->store.object("sports-archive","../../outside.txt"));

    assertEquals(400,error.status());
    assertEquals("InvalidURI",error.code());
  }

  @Test
  void rejectsMismatchedChecksumAndRemovesStagingFile() {
    NearlineStore store=store("sports-archive","SPORTS");
    String wrong=Base64.getEncoder().encodeToString(new byte[16]);

    S3Exception error=assertThrows(S3Exception.class,() -> store.put("sports-archive","bad.txt",bytes("content"),7,Map.of("content-md5",wrong)));

    assertEquals("BadDigest",error.code());
    assertFalse(Files.exists(temp.resolve("SPORTS/bad.txt")));
    assertDoesNotThrow(() -> {try(var files=Files.list(temp.resolve("SPORTS"))){assertTrue(files.noneMatch(p->p.getFileName().toString().endsWith(".uploading")));}});
  }

  @Test
  void rejectsMalformedChecksumBeforeWriting() {
    NearlineStore store=store("sports-archive","SPORTS");

    S3Exception error=assertThrows(S3Exception.class,() -> store.put("sports-archive","bad.txt",bytes("content"),7,Map.of("x-amz-checksum-sha256","not-base64")));

    assertEquals("InvalidDigest",error.code());
    assertFalse(Files.exists(temp.resolve("SPORTS/bad.txt")));
  }

  @Test
  void ifNoneMatchCreatesNewObjectButPreventsOverwrite() throws Exception {
    NearlineStore store=store("sports-archive","SPORTS");
    store.put("sports-archive","create-only.txt",bytes("first"),5,Map.of(),null,"*");

    S3Exception error=assertThrows(S3Exception.class,() -> store.put("sports-archive","create-only.txt",bytes("second"),6,Map.of(),null,"*"));

    assertEquals(412,error.status());
    assertEquals("PreconditionFailed",error.code());
    assertEquals("first",Files.readString(temp.resolve("SPORTS/create-only.txt")));
  }

  @Test
  void ifMatchReplacesOnlyTheExpectedObjectVersion() throws Exception {
    NearlineStore store=store("sports-archive","SPORTS");
    NearlineStore.Stored first=store.put("sports-archive","conditional.txt",bytes("first"),5);

    store.put("sports-archive","conditional.txt",bytes("second"),6,Map.of(),"\""+first.etag()+"\"",null);
    S3Exception error=assertThrows(S3Exception.class,() -> store.put("sports-archive","conditional.txt",bytes("third"),5,Map.of(),"\""+first.etag()+"\"",null));

    assertEquals(412,error.status());
    assertEquals("PreconditionFailed",error.code());
    assertEquals("second",Files.readString(temp.resolve("SPORTS/conditional.txt")));
  }

  @Test
  void ifMatchRequiresAnExistingObject() {
    NearlineStore store=store("sports-archive","SPORTS");

    S3Exception error=assertThrows(S3Exception.class,() -> store.put("sports-archive","missing.txt",bytes("data"),4,Map.of(),"\"d41d8cd98f00b204e9800998ecf8427e\"",null));

    assertEquals(404,error.status());
    assertEquals("NoSuchKey",error.code());
  }

  @Test
  void createsAndPersistsDynamicBucketAsCategory() throws Exception {
    NearlineStore store=new NearlineStore(properties());

    String category=store.createBucket("news-archive");

    assertEquals("NEWS_ARCHIVE",category);
    assertTrue(Files.isDirectory(temp.resolve("NEWS_ARCHIVE")));
    assertTrue(Files.isRegularFile(temp.resolve(".gateway-buckets.properties")));
    NearlineStore reloaded=new NearlineStore(properties());
    assertEquals("NEWS_ARCHIVE",reloaded.category("news-archive"));
    assertTrue(reloaded.buckets().contains("news-archive"));
  }

  @Test
  void rejectsCreatingExistingConfiguredOrDynamicBucket() throws Exception {
    NearlineStore configured=store("sports-archive","SPORTS");
    S3Exception configuredError=assertThrows(S3Exception.class,()->configured.createBucket("sports-archive"));
    assertEquals(409,configuredError.status());
    assertEquals("BucketAlreadyOwnedByYou",configuredError.code());

    NearlineStore dynamic=new NearlineStore(properties());
    dynamic.createBucket("news-archive");
    S3Exception dynamicError=assertThrows(S3Exception.class,()->dynamic.createBucket("news-archive"));
    assertEquals(409,dynamicError.status());
    assertEquals("BucketAlreadyOwnedByYou",dynamicError.code());
  }

  @Test
  void rejectsDynamicCategoryCollision() {
    NearlineStore store=store("existing-alias","NEWS_ARCHIVE");

    S3Exception error=assertThrows(S3Exception.class,()->store.createBucket("news-archive"));

    assertEquals(409,error.status());
    assertEquals("BucketAlreadyExists",error.code());
    assertFalse(Files.exists(temp.resolve(".gateway-buckets.properties")));
  }

  @Test
  void rejectsInvalidDynamicBucketName() {
    NearlineStore store=new NearlineStore(properties());

    S3Exception error=assertThrows(S3Exception.class,()->store.createBucket("Invalid_Name"));

    assertEquals(400,error.status());
    assertEquals("InvalidBucketName",error.code());
  }

  private NearlineStore store(String bucket,String category) {
    GatewayProperties properties=properties();
    properties.getBuckets().put(bucket,category);
    return new NearlineStore(properties);
  }

  private GatewayProperties properties() {
    GatewayProperties properties=new GatewayProperties();
    properties.setNearlineRoot(temp);
    properties.setBuckets(new LinkedHashMap<>());
    return properties;
  }

  private ByteArrayInputStream bytes(String value) {
    return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
  }
}
