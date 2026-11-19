package org.studentdiscounts.moderation;

import jakarta.validation.constraints.*;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.node.ObjectNode;

@Service
class ReportService {
  enum Category {
    OUTDATED,
    CLOSED,
    DISCOUNT_STOPPED,
    MISLEADING,
    ABUSE,
    OTHER
  }

  enum Action {
    TRIAGE,
    ESCALATE,
    RESOLVE
  }

  record Create(
      @NotNull UUID submissionId,
      @NotNull Category category,
      @NotBlank @Size(max = 2000) String description) {}

  record Triage(@NotNull Action action, @NotBlank @Size(max = 2000) String reason) {}

  private final ReviewStore store;

  ReportService(ReviewStore store) {
    this.store = store;
  }

  @Transactional
  Map<String, Object> create(ReviewRules.Actor actor, String key, Create body, String correlation) {
    ApiException.require(actor.roles().contains("STUDENT"), 403, "STUDENT_REQUIRED");
    String scope = "POST /api/v1/reports",
        fingerprint = ReviewStore.hash(store.json().writeValueAsString(body));
    var replay = store.replay(actor, scope, key, fingerprint);
    if (replay != null) return replay;
    UUID id = ReviewStore.id();
    store
        .jdbc()
        .update(
            "insert into reports(id,reporter_id,submission_id,category,description)"
                + " values(?,?,?,?,?)",
            id,
            actor.id(),
            body.submissionId(),
            body.category().name(),
            body.description().trim());
    Map<String, Object> out = own(actor, id);
    emit(
        id,
        actor.id(),
        body.submissionId(),
        "ReportCreated",
        body.description().trim(),
        correlation);
    // A report is only an observation. It never changes any review/submission/catalog status.
    store.remember(actor, scope, key, fingerprint, out);
    return out;
  }

  @Transactional(readOnly = true)
  Map<String, Object> own(ReviewRules.Actor actor, UUID id) {
    var rows = store.records("select * from reports where id=? and reporter_id=?", id, actor.id());
    if (rows.isEmpty()) throw new ApiException(404, "REPORT_NOT_FOUND");
    return rows.getFirst();
  }

  @Transactional(readOnly = true)
  List<Map<String, Object>> ownList(ReviewRules.Actor actor, int limit, UUID before) {
    ReviewService.bound(limit);
    return store.records(
        "select * from reports where reporter_id=? and (?::uuid is null or id<?::uuid) order by id"
            + " desc limit ?",
        actor.id(),
        before,
        before,
        limit);
  }

  @Transactional(readOnly = true)
  List<Map<String, Object>> queue(int limit, UUID before) {
    ReviewService.bound(limit);
    return store.records(
        "select * from reports where state<>'RESOLVED' and (?::uuid is null or id<?::uuid) order by"
            + " id desc limit ?",
        before,
        before,
        limit);
  }

  @Transactional
  Map<String, Object> triage(
      UUID id, ReviewRules.Actor actor, String key, long ifMatch, Triage body, String correlation) {
    ApiException.require(actor.reviewer(), 403, "REVIEWER_REQUIRED");
    String scope = "POST /api/v1/moderation/reports/" + id + "/actions",
        fingerprint = ReviewStore.hash(store.json().writeValueAsString(body) + ":" + ifMatch);
    var replay = store.replay(actor, scope, key, fingerprint);
    if (replay != null) return replay;
    var rows = store.records("select * from reports where id=? for update", id);
    if (rows.isEmpty()) throw new ApiException(404, "REPORT_NOT_FOUND");
    var report = rows.getFirst();
    ApiException.require(
        ((Number) report.get("rowVersion")).longValue() == ifMatch, 412, "STALE_ROW_VERSION");
    ApiException.require(!report.get("state").equals("RESOLVED"), 409, "REPORT_RESOLVED");
    ApiException.require(
        !actor.id().equals(report.get("reporterId")), 403, "SELF_TRIAGE_FORBIDDEN");
    if (body.action() == Action.RESOLVE)
      ApiException.require(actor.senior(), 403, "SENIOR_REQUIRED");
    if (report.get("state").equals("ESCALATED"))
      ApiException.require(actor.senior(), 403, "SENIOR_REQUIRED");
    UUID assignee = (UUID) report.get("assignedTo");
    ApiException.require(
        assignee == null || actor.id().equals(assignee) || actor.senior(),
        403,
        "ASSIGNED_REVIEWER_REQUIRED");
    String state =
        switch (body.action()) {
          case TRIAGE -> "TRIAGED";
          case ESCALATE -> "ESCALATED";
          case RESOLVE -> "RESOLVED";
        };
    store
        .jdbc()
        .update(
            "update reports set"
                + " state=?,assigned_to=?,resolution=?,row_version=row_version+1,updated_at=now()"
                + " where id=?",
            state,
            body.action() == Action.ESCALATE ? null : actor.id(),
            body.reason().trim(),
            id);
    String type =
        switch (body.action()) {
          case TRIAGE -> "ReportTriaged";
          case ESCALATE -> "ReportEscalated";
          case RESOLVE -> "ReportResolved";
        };
    emit(
        id, actor.id(), (UUID) report.get("submissionId"), type, body.reason().trim(), correlation);
    Map<String, Object> out = store.records("select * from reports where id=?", id).getFirst();
    store.remember(actor, scope, key, fingerprint, out);
    return out;
  }

  private void emit(
      UUID id, UUID actor, UUID submission, String type, String reason, String correlation) {
    ObjectNode data =
        store
            .json()
            .createObjectNode()
            .put("reportId", id.toString())
            .put("submissionId", submission.toString())
            .put("actorId", actor.toString())
            .put("reason", reason);
    data.put(
        "category",
        store.jdbc().queryForObject("select category from reports where id=?", String.class, id));
    UUID event = ReviewStore.id();
    store.outbox(event, id, "moderation.events.v1", store.envelope(event, type, correlation, data));
    store.audit(submission, actor, type, Map.of("reportId", id, "reason", reason), correlation);
  }
}
