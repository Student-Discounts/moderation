package org.studentdiscounts.moderation;

import com.fasterxml.uuid.Generators;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

@Repository
class ReviewStore {
  private final JdbcTemplate jdbc;
  private final ObjectMapper json;
  private static final com.fasterxml.uuid.impl.TimeBasedEpochGenerator IDS =
      Generators.timeBasedEpochGenerator();

  JdbcTemplate jdbc() {
    return jdbc;
  }

  ObjectMapper json() {
    return json;
  }

  ReviewStore(JdbcTemplate jdbc, ObjectMapper json) {
    this.jdbc = jdbc;
    this.json = json;
  }

  static UUID id() {
    return IDS.generate();
  }

  static String hash(String text) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  record Row(
      UUID id,
      UUID author,
      UUID versionId,
      int versionNo,
      long rowVersion,
      long sequence,
      long localVersion,
      String status,
      UUID reviewer,
      UUID pending,
      boolean escalated) {
    ReviewRules.State state() {
      return new ReviewRules.State(
          author,
          reviewer,
          versionId,
          versionNo,
          localVersion,
          rowVersion,
          status,
          pending != null,
          escalated);
    }
  }

  Row row(UUID id, boolean lock) {
    var rows =
        jdbc.query(
            "select * from reviews where submission_id=?" + (lock ? " for update" : ""),
            (r, n) ->
                new Row(
                    r.getObject("submission_id", UUID.class),
                    r.getObject("author_id", UUID.class),
                    r.getObject("version_id", UUID.class),
                    r.getInt("version_no"),
                    r.getLong("submission_row_version"),
                    r.getLong("last_sequence"),
                    r.getLong("local_version"),
                    r.getString("submission_status"),
                    r.getObject("reviewer_id", UUID.class),
                    r.getObject("pending_command_id", UUID.class),
                    r.getBoolean("escalated")),
            id);
    if (rows.isEmpty()) throw new ApiException(404, "REVIEW_NOT_FOUND");
    return rows.getFirst();
  }

  Map<String, Object> summary(Row r) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("submissionId", r.id());
    out.put("authorId", r.author());
    out.put("versionId", r.versionId());
    out.put("versionNo", r.versionNo());
    out.put("status", r.status());
    out.put("submissionRowVersion", r.rowVersion());
    out.put("localVersion", r.localVersion());
    out.put("reviewerId", r.reviewer());
    out.put("pendingCommandId", r.pending());
    out.put("escalated", r.escalated());
    return out;
  }

  List<Map<String, Object>> records(String sql, Object... args) {
    return jdbc.query(sql, (rs, n) -> record(rs), args);
  }

  private Map<String, Object> record(ResultSet rs) throws SQLException {
    Map<String, Object> out = new LinkedHashMap<>();
    var meta = rs.getMetaData();
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      String column = meta.getColumnLabel(i);
      StringBuilder key = new StringBuilder();
      boolean upper = false;
      for (char c : column.toCharArray()) {
        if (c == '_') upper = true;
        else {
          key.append(upper ? Character.toUpperCase(c) : c);
          upper = false;
        }
      }
      Object value = rs.getObject(i);
      if (meta.getColumnTypeName(i).equals("jsonb") && value != null)
        value = json.readTree(value.toString());
      if (value instanceof java.sql.Timestamp t) value = t.toInstant();
      out.put(key.toString(), value);
    }
    return out;
  }

  JsonNode payload(UUID version) {
    return json.readTree(
        jdbc.queryForObject(
            "select payload::text from snapshots where version_id=?", String.class, version));
  }

  void audit(UUID submission, UUID actor, String action, Object detail, String correlation) {
    jdbc.update(
        "insert into audit(id,submission_id,actor_id,action,detail,correlation_id)"
            + " values(?,?,?,?,?::jsonb,?)",
        id(),
        submission,
        actor,
        action,
        json.writeValueAsString(detail),
        correlation);
  }

  ObjectNode envelope(UUID eventId, String type, String correlation, JsonNode data) {
    ObjectNode e = json.createObjectNode();
    e.put("eventId", eventId.toString())
        .put("eventType", type)
        .put("schemaVersion", 1)
        .put("producer", "moderation")
        .put("occurredAt", java.time.Instant.now().toString())
        .put("correlationId", correlation);
    e.set("data", data);
    return e;
  }

  void outbox(UUID eventId, UUID aggregate, String topic, ObjectNode event) {
    long sequence =
        jdbc.queryForObject(
            "select coalesce(max(aggregate_sequence),0)+1 from outbox where aggregate_id=?",
            Long.class,
            aggregate);
    // The workflow schema forbids an aggregateSequence envelope member. Ordering lives in our
    // outbox.
    jdbc.update(
        "insert into outbox(id,aggregate_id,aggregate_sequence,topic,payload)"
            + " values(?,?,?,?,?::jsonb)",
        eventId,
        aggregate,
        sequence,
        topic,
        event.toString());
  }

  Map<String, Object> replay(
      ReviewRules.Actor actor, String scope, String key, String fingerprint) {
    ApiException.require(
        key != null && key.matches("[A-Za-z0-9._:-]{1,128}"), 400, "IDEMPOTENCY_KEY_REQUIRED");
    jdbc.queryForObject(
        "select pg_advisory_xact_lock(hashtextextended(?,0))",
        Object.class,
        actor.id() + ":" + scope + ":" + key);
    var rows =
        records(
            "select fingerprint,response from idempotency where actor_id=? and scope=? and key=?",
            actor.id(),
            scope,
            key);
    if (rows.isEmpty()) return null;
    ApiException.require(
        fingerprint.equals(rows.getFirst().get("fingerprint")), 409, "IDEMPOTENCY_KEY_REUSED");
    @SuppressWarnings("unchecked")
    Map<String, Object> response = json.convertValue(rows.getFirst().get("response"), Map.class);
    return response;
  }

  void remember(
      ReviewRules.Actor actor,
      String scope,
      String key,
      String fingerprint,
      Map<String, Object> response) {
    jdbc.update(
        "insert into idempotency(actor_id,scope,key,fingerprint,response) values(?,?,?,?,?::jsonb)",
        actor.id(),
        scope,
        key,
        fingerprint,
        json.writeValueAsString(response));
  }
}
