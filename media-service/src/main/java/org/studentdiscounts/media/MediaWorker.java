package org.studentdiscounts.media;

import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MediaWorker {
  private static final Logger LOG = LoggerFactory.getLogger(MediaWorker.class);
  private final MediaService service;
  private final PrivateStorage storage;
  private final MediaProperties properties;
  private final ImagePipeline pipeline;

  public MediaWorker(MediaService service, PrivateStorage storage, MediaProperties properties) {
    this.service = service;
    this.storage = storage;
    this.properties = properties;
    pipeline =
        new ImagePipeline(properties.limits().maxDimension(), properties.limits().maxPixels());
  }

  // One decoder per process; no in-memory work queue. DB leases recover after process termination.
  @Scheduled(fixedDelayString = "${SD_MEDIA_WORKER_DELAY_MS:500}")
  public void processNext() {
    if (!properties.workersEnabled()) return;
    MediaService.Asset claim;
    try {
      claim = service.claim();
    } catch (RuntimeException e) {
      LOG.warn("Media job claim unavailable");
      return;
    }
    if (claim == null) return;
    try {
      byte[] raw = storage.get(claim.originalKey(), properties.limits().maxBytes());
      if (raw.length != claim.rawBytes() || !MediaService.hash(raw).equals(claim.rawSha256()))
        throw new IllegalStateException("Stored quarantine object integrity failure");
      ImagePipeline.CleanImage clean;
      try {
        clean = pipeline.sanitize(raw, claim.filename(), claim.mime());
      } catch (ImagePipeline.RejectedImage rejection) {
        service.complete(claim, null, null, rejection.getMessage());
        return;
      } finally {
        Arrays.fill(raw, (byte) 0);
      }
      String key = "clean/" + claim.id() + "/" + claim.claimId() + ".png";
      storage.put(key, clean.png(), "image/png");
      service.complete(claim, clean, key, null);
    } catch (RuntimeException e) {
      LOG.warn("Media processing infrastructure unavailable asset={}", claim.id());
      try {
        service.retry(claim);
      } catch (RuntimeException failure) {
        LOG.warn("Media retry persistence unavailable asset={}", claim.id());
      }
    }
  }
}
