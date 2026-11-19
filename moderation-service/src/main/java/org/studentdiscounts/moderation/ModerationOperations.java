package org.studentdiscounts.moderation;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
class ModerationOperations {
  private final ReviewStore store;

  ModerationOperations(ReviewStore store, MeterRegistry metrics) {
    this.store = store;
    metrics.gauge(
        "moderation.commands.pending",
        this,
        s -> s.number("select count(*) from commands where state='PENDING'"));
    metrics.gauge(
        "moderation.commands.oldest.seconds",
        this,
        s ->
            s.number(
                "select coalesce(extract(epoch from(now()-min(created_at))),0) from commands where"
                    + " state='PENDING'"));
    metrics.gauge(
        "moderation.outbox.pending",
        this,
        s -> s.number("select count(*) from outbox where published_at is null"));
    metrics.gauge(
        "moderation.outbox.oldest.seconds",
        this,
        s ->
            s.number(
                "select coalesce(extract(epoch from(now()-min(created_at))),0) from outbox where"
                    + " published_at is null"));
    metrics.gauge(
        "moderation.queue.pending",
        this,
        s -> s.number("select count(*) from reviews where submission_status='PENDING_REVIEW'"));
    metrics.gauge(
        "moderation.consumer.failures",
        this,
        s -> s.number("select count(*) from consumer_failures"));
  }

  private double number(String sql) {
    try {
      return store.jdbc().queryForObject(sql, Number.class).doubleValue();
    } catch (org.springframework.dao.DataAccessException unavailable) {
      return Double.NaN;
    }
  }

  @Scheduled(fixedDelay = 60000)
  void pruneRateWindows() {
    store
        .jdbc()
        .update(
            "delete from request_windows where minute<date_trunc('minute',now())-interval '2"
                + " minutes'");
  }

  @GetMapping("/api/v1/moderation/consumer-failures")
  @PreAuthorize("hasRole('SENIOR_MODERATOR')")
  List<Map<String, Object>> failures(@RequestParam(defaultValue = "20") int limit) {
    ReviewService.bound(limit);
    return store.records(
        "select * from consumer_failures order by occurred_at desc limit ?", limit);
  }
}
