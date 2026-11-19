package org.studentdiscounts.moderation;

import jakarta.validation.constraints.*;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

@Service
class ReviewService {
  private final ReviewStore store;

  ReviewService(ReviewStore store) {
    this.store = store;
  }

  record Command(
      @NotNull ReviewRules.Action action,
      @NotNull UUID versionId,
      @Min(1) int versionNo,
      @Size(max = 2000) String reason) {}

  @Transactional(readOnly = true)
  List<Map<String, Object>> queue(int limit, UUID before, boolean escalatedOnly) {
    bound(limit);
    return store.records(
        "select submission_id,version_id,version_no,submission_status as"
            + " status,local_version,reviewer_id,pending_command_id,escalated,created_at from"
            + " reviews where submission_status in ('PENDING_REVIEW','IN_REVIEW') and (?::uuid is"
            + " null or submission_id<?::uuid) and (not ? or escalated) order by submission_id desc"
            + " limit ?",
        before,
        before,
        escalatedOnly,
        limit);
  }

  @Transactional(readOnly = true)
  Map<String, Object> detail(UUID id) {
    ReviewStore.Row row = store.row(id, false);
    Map<String, Object> out = store.summary(row);
    out.put("currentSnapshot", row.versionId() == null ? null : snapshot(id, row.versionNo()));
    out.put(
        "commands",
        store.records(
            "select * from commands where submission_id=? order by created_at desc limit 100", id));
    out.put("riskAssessment", null);
    out.put("riskAssessmentAvailability", "NOT_PROVIDED");
    return out;
  }

  @Transactional(readOnly = true)
  Map<String, Object> snapshot(UUID id, int versionNo) {
    var rows =
        store.records(
            "select * from snapshots where submission_id=? and version_no=?", id, versionNo);
    if (rows.isEmpty()) throw new ApiException(404, "VERSION_NOT_FOUND");
    return rows.getFirst();
  }

  @Transactional(readOnly = true)
  List<Map<String, Object>> versions(UUID id, int limit, Integer before) {
    bound(limit);
    store.row(id, false);
    return store.records(
        "select * from snapshots where submission_id=? and (?::integer is null or version_no<?)"
            + " order by version_no desc limit ?",
        id,
        before,
        before,
        limit);
  }

  @Transactional(readOnly = true)
  List<Map<String, Object>> history(UUID id, int limit, UUID before) {
    bound(limit);
    store.row(id, false);
    return store.records(
        "select * from audit where submission_id=? and (?::uuid is null or id<?::uuid) order by id"
            + " desc limit ?",
        id,
        before,
        before,
        limit);
  }

  @Transactional(readOnly = true)
  List<Map<String, Object>> comparison(UUID id, int versionNo) {
    var current = snapshot(id, versionNo);
    JsonNode now = (JsonNode) current.get("payload");
    JsonNode previous =
        versionNo == 1
            ? store.json().createObjectNode()
            : (JsonNode) snapshot(id, versionNo - 1).get("payload");
    List<Map<String, Object>> diff = new ArrayList<>();
    diff("", previous, now, diff);
    return diff;
  }

  private void diff(
      String path, JsonNode before, JsonNode after, List<Map<String, Object>> result) {
    if (Objects.equals(before, after)) return;
    if (before != null && after != null && before.isObject() && after.isObject()) {
      TreeSet<String> keys = new TreeSet<>(before.propertyNames());
      keys.addAll(after.propertyNames());
      for (String key : keys)
        diff(
            path + "/" + key.replace("~", "~0").replace("/", "~1"),
            before.get(key),
            after.get(key),
            result);
    } else {
      Map<String, Object> item = new LinkedHashMap<>();
      item.put("path", path);
      item.put("before", before);
      item.put("after", after);
      result.add(item);
    }
  }

  @Transactional
  Map<String, Object> command(
      UUID id,
      ReviewRules.Actor actor,
      String key,
      long ifMatch,
      Command command,
      String correlation) {
    String scope = "POST /api/v1/moderation/reviews/" + id + "/commands";
    String fingerprint = ReviewStore.hash(store.json().writeValueAsString(command) + ":" + ifMatch);
    var replay = store.replay(actor, scope, key, fingerprint);
    if (replay != null) return replay;
    ReviewStore.Row row = store.row(id, true);
    ReviewRules.check(
        row.state(),
        actor,
        command.action(),
        command.versionId(),
        command.versionNo(),
        ifMatch,
        command.reason());
    UUID commandId = ReviewStore.id();
    if (command.action() == ReviewRules.Action.ESCALATE
        || (command.action() == ReviewRules.Action.CLAIM && row.status().equals("IN_REVIEW"))) {
      boolean escalate = command.action() == ReviewRules.Action.ESCALATE;
      store
          .jdbc()
          .update(
              "update reviews set"
                  + " reviewer_id=?,escalated=?,local_version=local_version+1,updated_at=now()"
                  + " where submission_id=?",
              escalate ? null : actor.id(),
              escalate || row.escalated(),
              id);
      store
          .jdbc()
          .update(
              "insert into decisions(id,submission_id,version_id,version_no,actor_id,action,reason)"
                  + " values(?,?,?,?,?,?,?)",
              commandId,
              id,
              row.versionId(),
              row.versionNo(),
              actor.id(),
              command.action().name(),
              command.reason());
      store.audit(
          id,
          actor.id(),
          command.action().name(),
          Map.of("versionId", row.versionId(), "reason", Objects.toString(command.reason(), "")),
          correlation);
      Map<String, Object> result = response(commandId, "CONFIRMED", row.localVersion() + 1);
      store.remember(actor, scope, key, fingerprint, result);
      return result;
    }
    String target = ReviewRules.target(command.action());
    store
        .jdbc()
        .update(
            "insert into"
                + " commands(id,submission_id,version_id,version_no,expected_submission_version,actor_id,action,target_status,state,reason,correlation_id)"
                + " values(?,?,?,?,?,?,?,?,'PENDING',?,?)",
            commandId,
            id,
            row.versionId(),
            row.versionNo(),
            row.rowVersion(),
            actor.id(),
            command.action().name(),
            target,
            command.reason(),
            correlation);
    store
        .jdbc()
        .update(
            "update reviews set"
                + " reviewer_id=?,pending_command_id=?,local_version=local_version+1,updated_at=now()"
                + " where submission_id=?",
            command.action() == ReviewRules.Action.CLAIM ? actor.id() : row.reviewer(),
            commandId,
            id);
    ObjectNode data = store.json().createObjectNode();
    data.put("submissionId", id.toString())
        .put("versionId", row.versionId().toString())
        .put("versionNo", row.versionNo())
        .put("expectedRowVersion", row.rowVersion())
        .put("actorId", actor.id().toString());
    if (command.action() == ReviewRules.Action.APPROVE)
      data.set("payload", store.payload(row.versionId()));
    else if (command.action() != ReviewRules.Action.CLAIM)
      data.put("reason", command.reason().trim());
    store.outbox(
        commandId,
        id,
        "submission.workflow.v1",
        store.envelope(commandId, ReviewRules.eventType(command.action()), correlation, data));
    store.audit(
        id,
        actor.id(),
        "COMMAND_REQUESTED",
        Map.of(
            "commandId",
            commandId,
            "action",
            command.action().name(),
            "versionId",
            row.versionId()),
        correlation);
    Map<String, Object> result = response(commandId, "PENDING", row.localVersion() + 1);
    store.remember(actor, scope, key, fingerprint, result);
    return result;
  }

  private Map<String, Object> response(UUID id, String state, long version) {
    // Normalize the response through the same JSON representation used for durable replay.
    @SuppressWarnings("unchecked")
    Map<String, Object> out =
        store
            .json()
            .readValue(
                store
                    .json()
                    .writeValueAsString(
                        Map.of(
                            "commandId", id.toString(), "state", state, "localVersion", version)),
                Map.class);
    return out;
  }

  static void bound(int limit) {
    ApiException.require(limit >= 1 && limit <= 100, 400, "LIMIT_OUT_OF_RANGE");
  }
}
