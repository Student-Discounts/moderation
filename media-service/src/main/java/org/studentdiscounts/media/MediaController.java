package org.studentdiscounts.media;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
public class MediaController {
  private final MediaService service;
  private final PrivateStorage storage;
  private final MediaProperties properties;

  public MediaController(MediaService service, PrivateStorage storage, MediaProperties properties) {
    this.service = service;
    this.storage = storage;
    this.properties = properties;
  }

  @PostMapping(value = "/api/v1/media-assets", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public ResponseEntity<MediaService.UploadReceipt> upload(
      @AuthenticationPrincipal Jwt jwt,
      @RequestHeader(value = "Idempotency-Key", required = false) String key,
      @RequestPart("file") MultipartFile file) {
    if (file.getSize() == 0 || file.getSize() > properties.limits().maxBytes())
      throw new ApiFailure(HttpStatus.PAYLOAD_TOO_LARGE, "UPLOAD_SIZE_INVALID");
    try {
      byte[] bytes;
      try (var input = file.getInputStream()) {
        bytes = input.readNBytes(properties.limits().maxBytes() + 1);
      }
      var receipt =
          service.upload(
              UUID.fromString(jwt.getSubject()),
              key,
              file.getOriginalFilename(),
              file.getContentType(),
              bytes);
      return ResponseEntity.accepted()
          .location(URI.create("/api/v1/media-assets/" + receipt.id()))
          .cacheControl(CacheControl.noStore().cachePrivate())
          .body(receipt);
    } catch (IOException e) {
      throw new ApiFailure(HttpStatus.BAD_REQUEST, "UPLOAD_READ_FAILED");
    }
  }

  @GetMapping("/api/v1/media-assets/{id}")
  public ResponseEntity<MediaService.Metadata> metadata(
      @PathVariable UUID id, @AuthenticationPrincipal Jwt jwt) {
    var asset = authorized(id, jwt);
    return ResponseEntity.ok()
        .cacheControl(CacheControl.noStore().cachePrivate())
        .body(asset.metadata());
  }

  @GetMapping(value = "/api/v1/media-assets/{id}/preview", produces = MediaType.IMAGE_PNG_VALUE)
  public ResponseEntity<byte[]> preview(@PathVariable UUID id, @AuthenticationPrincipal Jwt jwt) {
    var asset = authorized(id, jwt);
    if (!"CLEAN".equals(asset.status()))
      throw new ApiFailure(HttpStatus.CONFLICT, "PREVIEW_NOT_READY");
    byte[] bytes = storage.get(asset.cleanKey(), properties.limits().maxPixels() * 5 + 1048576);
    return ResponseEntity.ok()
        .contentType(MediaType.IMAGE_PNG)
        .contentLength(bytes.length)
        .cacheControl(CacheControl.noStore().cachePrivate())
        .header("X-Content-Type-Options", "nosniff")
        .header("Content-Disposition", "inline; filename=\"preview.png\"")
        .body(bytes);
  }

  @GetMapping("/media/v1/assets/{id}")
  public ResponseEntity<MediaService.ConsumerMetadata> consumerMetadata(@PathVariable UUID id) {
    var asset = service.find(id);
    return ResponseEntity.ok()
        .cacheControl(CacheControl.noStore().cachePrivate())
        .body(
            new MediaService.ConsumerMetadata(
                asset.id(), asset.ownerId(), asset.status(), asset.revision()));
  }

  private MediaService.Asset authorized(UUID id, Jwt jwt) {
    var asset = service.find(id);
    if (!MediaSecurity.canRead(asset.ownerId(), jwt)) throw ApiFailure.notFound();
    return asset;
  }
}
