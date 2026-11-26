package org.studentdiscounts.media;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.net.URI;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("media")
public record MediaProperties(
    @NotBlank @Pattern(regexp = "[\\x21-\\x7e]{32,256}") String serviceToken,
    @NotBlank String issuer,
    @NotBlank String audience,
    @NotNull URI jwksUri,
    @Valid @NotNull Storage storage,
    @Valid @NotNull Limits limits,
    boolean workersEnabled,
    boolean outboxEnabled,
    @NotBlank String workflowTopic) {
  @Override
  public String toString() {
    return "MediaProperties[credentials=<redacted>]";
  }

  public record Storage(
      @NotNull URI endpoint,
      @NotBlank String region,
      @NotBlank String bucket,
      @NotBlank String accessKey,
      @NotBlank String secretKey,
      boolean createBucket) {
    public Storage {
      if (endpoint != null
          && (!java.util.Set.of("http", "https").contains(endpoint.getScheme())
              || endpoint.getHost() == null
              || endpoint.getUserInfo() != null
              || endpoint.getQuery() != null
              || endpoint.getFragment() != null))
        throw new IllegalArgumentException(
            "S3 endpoint must be an HTTP(S) origin without credentials");
    }

    @Override
    public String toString() {
      return "Storage[credentials=<redacted>]";
    }
  }

  public record Limits(
      @Min(1) @Max(10485760) int maxBytes,
      @Min(1) @Max(8192) int maxDimension,
      @Min(1) @Max(32000000) long maxPixels,
      @Min(1) @Max(16) int uploadConcurrency,
      @Min(1) @Max(16) int previewConcurrency,
      @Min(1) @Max(10000) int requestsPerMinute,
      @Min(1) @Max(10000) int uploadsPerDay) {}
}
