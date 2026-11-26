package org.studentdiscounts.moderation;

import java.time.Instant;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;

@Service
class WorkflowService {
  static final Set<String> STATUSES =
      Set.of(
          "DRAFT",
          "SUBMITTED",
          "VALIDATING",
          "PENDING_REVIEW",
          "IN_REVIEW",
          "CHANGES_REQUESTED",
          "APPROVED",
          "REJECTED",
          "CANCELLED",
          "PUBLISHED",
          "SUSPENDED",
          "ARCHIVED");
  private final ReviewStore store;

  WorkflowService(ReviewStore store) {
    this.store = store;
  }

  static final class PoisonEvent extends RuntimeException {
    PoisonEvent(String message) {
      super(message);
    }
  }

  static final class SequenceGap extends RuntimeException {
    SequenceGap() {
      super("AGGREGATE_SEQUENCE_GAP");
    }
  }

  static void require(boolean value, String code) {
    if (!value) throw new PoisonEvent(code);
  }

  static UUID uuid(JsonNode node, String field) {
    try {
      String s = text(node, field);
      UUID id = UUID.fromString(s);
      require(id.toString().equalsIgnoreCase(s), "INVALID_UUID");
      return id;
    } catch (IllegalArgumentException e) {
      throw new PoisonEvent("INVALID_UUID_" + field);
    }
  }

  static String text(JsonNode node, String field) {
    require(node.hasNonNull(field) && node.get(field).isString(), "INVALID_" + field);
    return node.get(field).asString();
  }

  static long number(JsonNode node, String field) {
    require(
        node.hasNonNull(field)
            && node.get(field).isIntegralNumber()
            && node.get(field).canConvertToLong()
            && node.get(field).asLong() >= 0,
        "INVALID_" + field);
    return node.get(field).asLong();
  }

  private static int integer(JsonNode node, String field) {
    long n = number(node, field);
    require(n <= Integer.MAX_VALUE, "INVALID_" + field);
    return (int) n;
  }

  private void fields(JsonNode node, Set<String> allowed) {
    require(
        node != null && node.isObject() && allowed.containsAll(node.propertyNames()),
        "UNKNOWN_EVENT_FIELD");
  }

  @Transactional
  void accept(String value, String kafkaKey) {
    JsonNode e;
    try {
      e = store.json().readTree(value);
    } catch (RuntimeException invalid) {
      throw new PoisonEvent("MALFORMED_JSON");
    }
    fields(
        e,
        Set.of(
            "eventId",
            "eventType",
            "schemaVersion",
            "producer",
            "occurredAt",
            "correlationId",
            "aggregateSequence",
            "data"));
    require(
        number(e, "schemaVersion") == 1 && text(e, "producer").equals("submission"),
        "UNTRUSTED_EVENT");
    UUID eventId = uuid(e, "eventId");
    String type = text(e, "eventType"), correlation = text(e, "correlationId");
    require(correlation.matches("[A-Za-z0-9._:-]{1,64}"), "INVALID_CORRELATION");
    Instant occurred;
    try {
      occurred = Instant.parse(text(e, "occurredAt"));
    } catch (RuntimeException invalid) {
      throw new PoisonEvent("INVALID_OCCURRED_AT");
    }
    JsonNode d = e.path("data");
    UUID id = uuid(d, "submissionId");
    require(id.toString().equals(kafkaKey), "WRONG_KAFKA_KEY");
    long sequence = number(e, "aggregateSequence");
    require(sequence >= 1, "INVALID_SEQUENCE");
    String fingerprint = ReviewStore.hash(e.toString());
    store.jdbc().update("insert into reviews(submission_id) values(?) on conflict do nothing", id);
    ReviewStore.Row row = store.row(id, true);
    var seen = store.records("select fingerprint from inbox where event_id=?", eventId);
    if (!seen.isEmpty()) {
      require(fingerprint.equals(seen.getFirst().get("fingerprint")), "EVENT_ID_REUSED");
      return;
    }
    if (sequence > row.sequence() + 1) throw new SequenceGap();
    require(sequence == row.sequence() + 1, "SEQUENCE_REUSED");
    switch (type) {
      case "SubmissionSubmitted", "SubmissionVersionCreated" ->
          version(row, d, type, occurred, correlation);
      case "SubmissionStatusChanged" -> status(row, d, correlation);
      default -> throw new PoisonEvent("UNKNOWN_EVENT_TYPE");
    }
    store
        .jdbc()
        .update(
            "update reviews set last_sequence=?,local_version=local_version+1,updated_at=now()"
                + " where submission_id=?",
            sequence,
            id);
    store
        .jdbc()
        .update(
            "insert into inbox(event_id,source,aggregate_id,sequence,fingerprint)"
                + " values(?,'submission.events.v1',?,?,?)",
            eventId,
            id,
            sequence,
            fingerprint);
    store.audit(id, null, type, d, correlation);
  }

  private void version(
      ReviewStore.Row row, JsonNode d, String type, Instant occurred, String correlation) {
    fields(
        d,
        Set.of(
            "submissionId",
            "versionId",
            "versionNo",
            "payload",
            "payloadSchemaVersion",
            "contentSha256",
            "baseOfferRowVersion",
            "authorId",
            "rowVersion"));
    UUID version = uuid(d, "versionId"), author = uuid(d, "authorId");
    int no = integer(d, "versionNo");
    require(no >= 1, "INVALID_VERSION");
    long rv = number(d, "rowVersion");
    require(rv >= row.rowVersion(), "ROW_VERSION_REGRESSION");
    require(
        number(d, "payloadSchemaVersion") == 1 && d.path("payload").isObject(), "INVALID_SNAPSHOT");
    String hash = text(d, "contentSha256");
    require(hash.matches("[a-f0-9]{64}"), "INVALID_HASH");
    require(d.has("baseOfferRowVersion"), "MISSING_BASE_VERSION");
    Integer base = d.get("baseOfferRowVersion").isNull() ? null : integer(d, "baseOfferRowVersion");
    require(row.author() == null || row.author().equals(author), "AUTHOR_CHANGED");
    var snapshots = store.records("select * from snapshots where version_id=?", version);
    if (snapshots.isEmpty()) {
      require(no == row.versionNo() + 1, "VERSION_SEQUENCE_GAP");
      store
          .jdbc()
          .update(
              "insert into"
                  + " snapshots(version_id,submission_id,version_no,payload,payload_schema_version,content_sha256,base_offer_row_version,author_id,occurred_at)"
                  + " values(?,?,?,?::jsonb,1,?,?,?,?)",
              version,
              row.id(),
              no,
              d.get("payload").toString(),
              hash,
              base,
              author,
              java.sql.Timestamp.from(occurred));
    } else {
      var existing = snapshots.getFirst();
      require(
          existing.get("submissionId").equals(row.id())
              && ((Number) existing.get("versionNo")).intValue() == no
              && Objects.equals(existing.get("contentSha256"), hash)
              && existing.get("payload").equals(d.get("payload"))
              && Objects.equals(existing.get("baseOfferRowVersion"), base)
              && existing.get("authorId").equals(author),
          "IMMUTABLE_VERSION_CONFLICT");
      require(no == row.versionNo(), "STALE_VERSION_EVENT");
    }
    supersede(row, "NEW_VERSION_OR_SUBMISSION", correlation);
    store
        .jdbc()
        .update(
            "update reviews set"
                + " author_id=?,version_id=?,version_no=?,submission_row_version=?,submission_status=?,reviewer_id=null,escalated=false"
                + " where submission_id=?",
            author,
            version,
            no,
            rv,
            type.equals("SubmissionSubmitted") ? "SUBMITTED" : row.status(),
            row.id());
  }

  private void status(ReviewStore.Row row, JsonNode d, String correlation) {
    fields(
        d,
        Set.of(
            "submissionId",
            "versionId",
            "versionNo",
            "fromStatus",
            "toStatus",
            "rowVersion",
            "expectedRowVersion",
            "actorId",
            "reason"));
    String target = text(d, "toStatus");
    require(STATUSES.contains(target), "INVALID_STATUS");
    long rv = number(d, "rowVersion");
    require(rv >= row.rowVersion(), "ROW_VERSION_REGRESSION");
    if (d.hasNonNull("versionId"))
      require(
          uuid(d, "versionId").equals(row.versionId()) && number(d, "versionNo") == row.versionNo(),
          "UNKNOWN_VERSION");
    else require(row.versionId() == null, "MISSING_VERSION");
    if (row.pending() != null) {
      Map<String, Object> command =
          store.records("select * from commands where id=?", row.pending()).getFirst();
      boolean confirmed =
          command.get("targetStatus").equals(target)
              && ((Number) command.get("expectedSubmissionVersion")).longValue() + 1 == rv
              && d.hasNonNull("actorId")
              && command.get("actorId").equals(uuid(d, "actorId"))
              && command.get("versionId").equals(row.versionId());
      if (confirmed) {
        store
            .jdbc()
            .update(
                "update commands set state='CONFIRMED',resolved_at=now() where id=?",
                row.pending());
        store
            .jdbc()
            .update(
                "insert into"
                    + " decisions(id,command_id,submission_id,version_id,version_no,actor_id,action,reason)"
                    + " values(?,?,?,?,?,?,?,?)",
                ReviewStore.id(),
                row.pending(),
                row.id(),
                row.versionId(),
                row.versionNo(),
                command.get("actorId"),
                command.get("action"),
                command.get("reason"));
        store
            .jdbc()
            .update("update reviews set pending_command_id=null where submission_id=?", row.id());
        store.audit(
            row.id(),
            (UUID) command.get("actorId"),
            "COMMAND_CONFIRMED",
            Map.of("commandId", row.pending()),
            correlation);
      } else if (rv > ((Number) command.get("expectedSubmissionVersion")).longValue())
        supersede(row, "CONFLICTING_COMMITTED_TRANSITION", correlation);
    }
    store
        .jdbc()
        .update(
            "update reviews set submission_status=?,submission_row_version=?,reviewer_id=case when"
                + " ?='IN_REVIEW' then reviewer_id else null end where submission_id=?",
            target,
            rv,
            target,
            row.id());
  }

  private void supersede(ReviewStore.Row row, String reason, String correlation) {
    if (row.pending() != null) {
      store
          .jdbc()
          .update(
              "update commands set state='SUPERSEDED',failure=?,resolved_at=now() where id=? and"
                  + " state='PENDING'",
              reason,
              row.pending());
      store.audit(
          row.id(),
          null,
          "COMMAND_SUPERSEDED",
          Map.of("commandId", row.pending(), "reason", reason),
          correlation);
    }
    store
        .jdbc()
        .update(
            "update reviews set pending_command_id=null,reviewer_id=null where submission_id=?",
            row.id());
  }

  @Transactional
  void rejected(String value, String source) {
    JsonNode event;
    try {
      event = store.json().readTree(value);
    } catch (RuntimeException malformed) {
      throw new PoisonEvent("MALFORMED_DLT");
    }
    require(event != null && event.isObject(), "MALFORMED_DLT");
    // The shared DLT contains other producers too. Only our exact persisted command is relevant.
    if (!event.path("producer").asString("").equals("moderation")) return;
    UUID eventId = uuid(event, "eventId");
    var commands = store.records("select submission_id from commands where id=?", eventId);
    if (commands.isEmpty()) return;
    UUID id = (UUID) commands.getFirst().get("submissionId");
    ReviewStore.Row row = store.row(id, true);
    JsonNode original =
        store
            .json()
            .readTree(
                store
                    .jdbc()
                    .queryForObject(
                        "select payload::text from outbox where id=?", String.class, eventId));
    require(original.equals(event), "DLT_COMMAND_MISMATCH");
    UUID inboxId =
        UUID.nameUUIDFromBytes(
            ("DLT:" + eventId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    int inserted =
        store
            .jdbc()
            .update(
                "insert into inbox(event_id,source,aggregate_id,fingerprint) values(?,?,?,?) on"
                    + " conflict do nothing",
                inboxId,
                source,
                id,
                ReviewStore.hash(event.toString()));
    if (inserted == 0) return;
    int changed =
        store
            .jdbc()
            .update(
                "update commands set"
                    + " state='REJECTED',failure='SUBMISSION_WORKFLOW_DLT',resolved_at=now() where"
                    + " id=? and state='PENDING'",
                eventId);
    if (changed == 1 && eventId.equals(row.pending())) {
      String action =
          store
              .jdbc()
              .queryForObject("select action from commands where id=?", String.class, eventId);
      store
          .jdbc()
          .update(
              "update reviews set pending_command_id=null,reviewer_id=case when ?='CLAIM' then null"
                  + " else reviewer_id end,local_version=local_version+1,updated_at=now() where"
                  + " submission_id=?",
              action,
              id);
      store.audit(
          id,
          null,
          "COMMAND_REJECTED",
          Map.of("commandId", eventId, "source", source),
          text(event, "correlationId"));
    }
  }
}
