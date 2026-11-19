package org.studentdiscounts.media;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

@Component("privateStorage")
public class PrivateStorage implements HealthIndicator {
  private final S3Client s3;
  private final String bucket;
  private final boolean createBucket;

  public PrivateStorage(MediaProperties properties) {
    var config = properties.storage();
    bucket = config.bucket();
    createBucket = config.createBucket();
    s3 =
        S3Client.builder()
            .endpointOverride(config.endpoint())
            .region(Region.of(config.region()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.accessKey(), config.secretKey())))
            .forcePathStyle(true)
            .httpClientBuilder(
                UrlConnectionHttpClient.builder()
                    .connectionTimeout(Duration.ofSeconds(2))
                    .socketTimeout(Duration.ofSeconds(10)))
            .overrideConfiguration(c ->
                c.apiCallTimeout(Duration.ofSeconds(20))
                    .apiCallAttemptTimeout(Duration.ofSeconds(10)))
            .build();
  }

  @PostConstruct
  void initialize() {
    if (createBucket) {
      try {
        s3.headBucket(b -> b.bucket(bucket));
      } catch (NoSuchBucketException e) {
        s3.createBucket(b -> b.bucket(bucket));
      } catch (S3Exception e) {
        if (e.statusCode() == 404) s3.createBucket(b -> b.bucket(bucket));
        else throw e;
      }
    }
    // A public bucket is a fatal configuration error; no public URLs or presigned originals are
    // used.
    var acl = s3.getBucketAcl(b -> b.bucket(bucket));
    for (Grant grant : acl.grants()) {
      String uri = grant.grantee().uri();
      if (uri != null && (uri.endsWith("/AllUsers") || uri.endsWith("/AuthenticatedUsers")))
        throw new IllegalStateException("Media bucket must be private");
    }
    try {
      String policy = s3.getBucketPolicy(b -> b.bucket(bucket)).policy();
      if (policy != null && !policy.isBlank())
        throw new IllegalStateException("Use identity policies, not a media bucket policy");
    } catch (S3Exception e) {
      if (!"NoSuchBucketPolicy".equals(e.awsErrorDetails().errorCode()) && e.statusCode() != 404)
        throw e;
    }
  }

  public void put(String key, byte[] data, String type) {
    s3.putObject(
        b -> b.bucket(bucket).key(key).contentType(type).cacheControl("private, no-store"),
        RequestBody.fromContentProvider(
            () -> new java.io.ByteArrayInputStream(data), data.length, type));
  }

  public byte[] get(String key, long maxBytes) {
    var response = s3.getObject(b -> b.bucket(bucket).key(key));
    try (response) {
      if (response.response().contentLength() == null
          || response.response().contentLength() > maxBytes) throw ApiFailure.unavailable();
      byte[] bytes = response.readNBytes(Math.toIntExact(maxBytes) + 1);
      if (bytes.length > maxBytes) throw ApiFailure.unavailable();
      return bytes;
    } catch (java.io.IOException e) {
      throw ApiFailure.unavailable();
    }
  }

  public void delete(String key) {
    s3.deleteObject(b -> b.bucket(bucket).key(key));
  }

  @Override
  public Health health() {
    try {
      s3.headBucket(b -> b.bucket(bucket));
      return Health.up().build();
    } catch (RuntimeException e) {
      return Health.down().build();
    }
  }

  @PreDestroy
  void close() {
    s3.close();
  }
}
