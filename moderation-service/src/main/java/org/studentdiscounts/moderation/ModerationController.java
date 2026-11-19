package org.studentdiscounts.moderation;

import jakarta.validation.Valid;
import java.net.URI;
import java.util.*;
import org.slf4j.MDC;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;

@RestController
class ModerationController {
  private final ReviewService reviews;
  private final ReportService reports;

  ModerationController(ReviewService reviews, ReportService reports) {
    this.reviews = reviews;
    this.reports = reports;
  }

  static ReviewRules.Actor actor(Jwt jwt) {
    List<String> roles = jwt.getClaimAsStringList("roles");
    return new ReviewRules.Actor(
        UUID.fromString(jwt.getSubject()), roles == null ? Set.of() : Set.copyOf(roles));
  }

  static long etag(String value) {
    if (value == null) throw new ApiException(428, "IF_MATCH_REQUIRED");
    ApiException.require(value.matches("\"[0-9]{1,18}\""), 400, "INVALID_IF_MATCH");
    return Long.parseLong(value.substring(1, value.length() - 1));
  }

  static String correlation() {
    return MDC.get("correlationId");
  }

  @GetMapping("/api/v1/moderation/queue")
  List<Map<String, Object>> queue(
      @RequestParam(defaultValue = "20") int limit,
      @RequestParam(required = false) UUID before,
      @RequestParam(defaultValue = "false") boolean escalatedOnly) {
    return reviews.queue(limit, before, escalatedOnly);
  }

  @GetMapping("/api/v1/moderation/reviews/{id}")
  ResponseEntity<Map<String, Object>> detail(@PathVariable UUID id) {
    var body = reviews.detail(id);
    return ResponseEntity.ok().eTag(body.get("localVersion").toString()).body(body);
  }

  @GetMapping("/api/v1/moderation/reviews/{id}/versions")
  List<Map<String, Object>> versions(
      @PathVariable UUID id,
      @RequestParam(defaultValue = "20") int limit,
      @RequestParam(required = false) Integer before) {
    return reviews.versions(id, limit, before);
  }

  @GetMapping("/api/v1/moderation/reviews/{id}/versions/{versionNo}")
  Map<String, Object> version(@PathVariable UUID id, @PathVariable int versionNo) {
    return reviews.snapshot(id, versionNo);
  }

  @GetMapping("/api/v1/moderation/reviews/{id}/comparison")
  List<Map<String, Object>> comparison(@PathVariable UUID id, @RequestParam int versionNo) {
    return reviews.comparison(id, versionNo);
  }

  @GetMapping("/api/v1/moderation/reviews/{id}/history")
  List<Map<String, Object>> history(
      @PathVariable UUID id,
      @RequestParam(defaultValue = "20") int limit,
      @RequestParam(required = false) UUID before) {
    return reviews.history(id, limit, before);
  }

  @PostMapping("/api/v1/moderation/reviews/{id}/commands")
  ResponseEntity<Map<String, Object>> command(
      @PathVariable UUID id,
      @AuthenticationPrincipal Jwt jwt,
      @RequestHeader(value = "If-Match", required = false) String ifMatch,
      @RequestHeader(value = "Idempotency-Key", required = false) String key,
      @Valid @RequestBody ReviewService.Command command) {
    var response = reviews.command(id, actor(jwt), key, etag(ifMatch), command, correlation());
    return ResponseEntity.accepted().eTag(response.get("localVersion").toString()).body(response);
  }

  @PostMapping("/api/v1/reports")
  ResponseEntity<Map<String, Object>> createReport(
      @AuthenticationPrincipal Jwt jwt,
      @RequestHeader(value = "Idempotency-Key", required = false) String key,
      @Valid @RequestBody ReportService.Create body) {
    var response = reports.create(actor(jwt), key, body, correlation());
    return ResponseEntity.created(URI.create("/api/v1/reports/" + response.get("id")))
        .eTag(response.get("rowVersion").toString())
        .body(response);
  }

  @GetMapping("/api/v1/reports")
  List<Map<String, Object>> ownReports(
      @AuthenticationPrincipal Jwt jwt,
      @RequestParam(defaultValue = "20") int limit,
      @RequestParam(required = false) UUID before) {
    return reports.ownList(actor(jwt), limit, before);
  }

  @GetMapping("/api/v1/reports/{id}")
  ResponseEntity<Map<String, Object>> report(
      @PathVariable UUID id, @AuthenticationPrincipal Jwt jwt) {
    var body = reports.own(actor(jwt), id);
    return ResponseEntity.ok().eTag(body.get("rowVersion").toString()).body(body);
  }

  @GetMapping("/api/v1/moderation/reports")
  List<Map<String, Object>> reportQueue(
      @RequestParam(defaultValue = "20") int limit, @RequestParam(required = false) UUID before) {
    return reports.queue(limit, before);
  }

  @PostMapping("/api/v1/moderation/reports/{id}/actions")
  ResponseEntity<Map<String, Object>> triage(
      @PathVariable UUID id,
      @AuthenticationPrincipal Jwt jwt,
      @RequestHeader(value = "If-Match", required = false) String ifMatch,
      @RequestHeader(value = "Idempotency-Key", required = false) String key,
      @Valid @RequestBody ReportService.Triage body) {
    var response = reports.triage(id, actor(jwt), key, etag(ifMatch), body, correlation());
    return ResponseEntity.ok().eTag(response.get("rowVersion").toString()).body(response);
  }
}
