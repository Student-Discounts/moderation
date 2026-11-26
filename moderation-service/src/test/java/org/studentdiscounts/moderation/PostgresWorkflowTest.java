package org.studentdiscounts.moderation;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

@Testcontainers
@SpringBootTest(
    properties = {
      "moderation.listener-enabled=false",
      "moderation.outbox-enabled=false",
      "moderation.local-topics-enabled=false"
    })
class PostgresWorkflowTest {
  @Container
  static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

  @Container static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.1.0");

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry p) {
    p.add("spring.datasource.url", postgres::getJdbcUrl);
    p.add("spring.datasource.username", postgres::getUsername);
    p.add("spring.datasource.password", postgres::getPassword);
    p.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    p.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> "http://localhost:18080");
    p.add(
        "spring.security.oauth2.resourceserver.jwt.jwk-set-uri",
        () -> "http://localhost:18080/.well-known/jwks.json");
  }

  @Autowired WorkflowService workflow;
  @Autowired ReviewService reviews;
  @Autowired JdbcTemplate jdbc;
  @Autowired ObjectMapper json;
  @Autowired ReportService reports;
  final UUID actor = UUID.randomUUID();
  final ReviewRules.Actor moderator = new ReviewRules.Actor(actor, Set.of("MODERATOR"));

  ObjectNode event(
      UUID id, UUID version, UUID author, int no, long seq, String type, String status, long row) {
    ObjectNode e = json.createObjectNode();
    e.put("eventId", UUID.randomUUID().toString())
        .put("eventType", type)
        .put("schemaVersion", 1)
        .put("producer", "submission")
        .put("occurredAt", "2026-11-19T12:00:00Z")
        .put("correlationId", "test")
        .put("aggregateSequence", seq);
    ObjectNode d = e.putObject("data");
    d.put("submissionId", id.toString())
        .put("versionId", version.toString())
        .put("versionNo", no)
        .put("rowVersion", row);
    if (type.equals("SubmissionStatusChanged")) {
      d.put("fromStatus", "VALIDATING").put("toStatus", status);
    } else {
      d.put("authorId", author.toString())
          .put("payloadSchemaVersion", 1)
          .put("contentSha256", "a".repeat(64));
      d.putNull("baseOfferRowVersion");
      d.putObject("payload").put("name", "immutable");
    }
    return e;
  }

  UUID[] seed() {
    UUID id = UUID.randomUUID(), v = UUID.randomUUID(), author = UUID.randomUUID();
    workflow.accept(
        event(id, v, author, 1, 1, "SubmissionSubmitted", null, 1).toString(), id.toString());
    workflow.accept(
        event(id, v, author, 1, 2, "SubmissionStatusChanged", "PENDING_REVIEW", 3).toString(),
        id.toString());
    return new UUID[] {id, v, author};
  }

  @Test
  void inboxDuplicateAndImmutableSnapshotAreTransactional() {
    UUID id = UUID.randomUUID(), v = UUID.randomUUID();
    String e = event(id, v, UUID.randomUUID(), 1, 1, "SubmissionSubmitted", null, 1).toString();
    workflow.accept(e, id.toString());
    workflow.accept(e, id.toString());
    assertEquals(
        1,
        jdbc.queryForObject("select count(*) from inbox where aggregate_id=?", Integer.class, id));
    assertThrows(
        RuntimeException.class,
        () -> jdbc.update("update snapshots set payload='{}' where version_id=?", v));
    assertThrows(
        RuntimeException.class, () -> jdbc.update("delete from audit where submission_id=?", id));
  }

  @Test
  void claimIsPendingUntilExactCommittedConfirmationAndStaleDecisionRollsBack() {
    UUID[] s = seed();
    long etag = ((Number) reviews.detail(s[0]).get("localVersion")).longValue();
    var response =
        reviews.command(
            s[0],
            moderator,
            "claim-key",
            etag,
            new ReviewService.Command(ReviewRules.Action.CLAIM, s[1], 1, null),
            "test");
    assertEquals("PENDING", response.get("state"));
    assertEquals(
        response,
        reviews.command(
            s[0],
            moderator,
            "claim-key",
            etag,
            new ReviewService.Command(ReviewRules.Action.CLAIM, s[1], 1, null),
            "test"));
    long current = ((Number) reviews.detail(s[0]).get("localVersion")).longValue();
    assertThrows(
        ApiException.class,
        () ->
            reviews.command(
                s[0],
                moderator,
                "too-early",
                current,
                new ReviewService.Command(ReviewRules.Action.APPROVE, s[1], 1, null),
                "test"));
    ObjectNode confirmed = event(s[0], s[1], s[2], 1, 3, "SubmissionStatusChanged", "IN_REVIEW", 4);
    ((ObjectNode) confirmed.get("data"))
        .put("actorId", actor.toString())
        .put("expectedRowVersion", 3);
    workflow.accept(confirmed.toString(), s[0].toString());
    assertEquals(
        "CONFIRMED",
        jdbc.queryForObject(
            "select state from commands where id=?",
            String.class,
            UUID.fromString(response.get("commandId").toString())));
    long count =
        jdbc.queryForObject("select count(*) from outbox where aggregate_id=?", Long.class, s[0]);
    assertThrows(
        ApiException.class,
        () ->
            reviews.command(
                s[0],
                moderator,
                "stale",
                etag,
                new ReviewService.Command(ReviewRules.Action.APPROVE, s[1], 1, null),
                "test"));
    assertEquals(
        count,
        jdbc.queryForObject("select count(*) from outbox where aggregate_id=?", Long.class, s[0]));
  }

  @Test
  void concurrentClaimsHaveExactlyOneWinnerAndOneOutbox() throws Exception {
    UUID[] s = seed();
    long etag = ((Number) reviews.detail(s[0]).get("localVersion")).longValue();
    try (var pool = Executors.newFixedThreadPool(2)) {
      Callable<Boolean> claim =
          () -> {
            try {
              reviews.command(
                  s[0],
                  new ReviewRules.Actor(UUID.randomUUID(), Set.of("MODERATOR")),
                  UUID.randomUUID().toString(),
                  etag,
                  new ReviewService.Command(ReviewRules.Action.CLAIM, s[1], 1, null),
                  "test");
              return true;
            } catch (ApiException conflict) {
              return false;
            }
          };
      var results = pool.invokeAll(List.of(claim, claim));
      assertEquals(
          1,
          results.stream()
              .filter(r -> {
                try {
                  return r.get();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              })
              .count());
    }
    assertEquals(
        1,
        jdbc.queryForObject(
            "select count(*) from commands where submission_id=?", Integer.class, s[0]));
    assertEquals(
        1,
        jdbc.queryForObject(
            "select count(*) from outbox where aggregate_id=?", Integer.class, s[0]));
  }

  @Test
  void gapRollsBackInboxAndNewVersionCancelsAssignment() {
    UUID[] s = seed();
    assertThrows(
        RuntimeException.class,
        () ->
            workflow.accept(
                event(s[0], s[1], s[2], 1, 4, "SubmissionStatusChanged", "IN_REVIEW", 4).toString(),
                s[0].toString()));
    assertEquals(
        2,
        jdbc.queryForObject(
            "select count(*) from inbox where aggregate_id=?", Integer.class, s[0]));
    long etag = ((Number) reviews.detail(s[0]).get("localVersion")).longValue();
    reviews.command(
        s[0],
        moderator,
        "new-version",
        etag,
        new ReviewService.Command(ReviewRules.Action.CLAIM, s[1], 1, null),
        "test");
    workflow.accept(
        event(s[0], UUID.randomUUID(), s[2], 2, 3, "SubmissionVersionCreated", null, 4).toString(),
        s[0].toString());
    assertNull(
        jdbc.queryForObject(
            "select reviewer_id from reviews where submission_id=?", UUID.class, s[0]));
    assertEquals(
        "SUPERSEDED",
        jdbc.queryForObject(
            "select state from commands where submission_id=?", String.class, s[0]));
  }

  @Test
  void dltRejectsOnlyExactPendingCommand() {
    UUID[] s = seed();
    long etag = ((Number) reviews.detail(s[0]).get("localVersion")).longValue();
    var response =
        reviews.command(
            s[0],
            moderator,
            "dlt",
            etag,
            new ReviewService.Command(ReviewRules.Action.CLAIM, s[1], 1, null),
            "test");
    String payload =
        jdbc.queryForObject(
            "select payload::text from outbox where id=?",
            String.class,
            UUID.fromString(response.get("commandId").toString()));
    workflow.rejected(payload, "submission.workflow.v1.DLT");
    workflow.rejected(payload, "submission.workflow.v1.DLT");
    assertEquals(
        "REJECTED",
        jdbc.queryForObject(
            "select state from commands where submission_id=?", String.class, s[0]));
    assertNull(
        jdbc.queryForObject(
            "select reviewer_id from reviews where submission_id=?", UUID.class, s[0]));
  }

  @Test
  void correctionInvalidatesOldDecisionAndAllowsLocalReclaimOfExactTuple() {
    UUID[] s = seed();
    long etag = ((Number) reviews.detail(s[0]).get("localVersion")).longValue();
    var claim =
        reviews.command(
            s[0],
            moderator,
            "correct-claim",
            etag,
            new ReviewService.Command(ReviewRules.Action.CLAIM, s[1], 1, null),
            "test");
    ObjectNode confirmed = event(s[0], s[1], s[2], 1, 3, "SubmissionStatusChanged", "IN_REVIEW", 4);
    ((ObjectNode) confirmed.get("data")).put("actorId", actor.toString());
    workflow.accept(confirmed.toString(), s[0].toString());
    UUID next = UUID.randomUUID();
    ObjectNode correction = event(s[0], next, s[2], 2, 4, "SubmissionVersionCreated", null, 5);
    ((ObjectNode) correction.path("data").path("payload")).put("name", "corrected");
    workflow.accept(correction.toString(), s[0].toString());
    long fresh = ((Number) reviews.detail(s[0]).get("localVersion")).longValue();
    assertThrows(
        ApiException.class,
        () ->
            reviews.command(
                s[0],
                moderator,
                "stale-tuple",
                fresh,
                new ReviewService.Command(ReviewRules.Action.APPROVE, s[1], 1, null),
                "test"));
    long before =
        jdbc.queryForObject("select count(*) from outbox where aggregate_id=?", Long.class, s[0]);
    var reclaim =
        reviews.command(
            s[0],
            moderator,
            "reclaim",
            fresh,
            new ReviewService.Command(ReviewRules.Action.CLAIM, next, 2, null),
            "test");
    assertEquals("CONFIRMED", reclaim.get("state"));
    assertEquals(
        before,
        jdbc.queryForObject("select count(*) from outbox where aggregate_id=?", Long.class, s[0]));
    assertEquals(actor, reviews.detail(s[0]).get("reviewerId"));
    assertEquals(false, reviews.detail(s[0]).get("escalated"));
    assertEquals(1, reviews.comparison(s[0], 2).size());
    assertThrows(
        RuntimeException.class,
        () ->
            jdbc.update(
                "delete from decisions where command_id=?",
                UUID.fromString(claim.get("commandId").toString())));
  }

  @Test
  void reportTriageRequiresExplicitSeniorResolutionAndNeverEmitsWorkflowCommand() {
    var student = new ReviewRules.Actor(UUID.randomUUID(), Set.of("STUDENT"));
    UUID target = UUID.randomUUID();
    var report =
        reports.create(
            student,
            "report",
            new ReportService.Create(
                target, ReportService.Category.CLOSED, "The location has closed"),
            "test");
    UUID id = (UUID) report.get("id");
    assertEquals("OPEN", report.get("state"));
    assertEquals(
        0,
        jdbc.queryForObject(
            "select count(*) from reviews where submission_id=?", Integer.class, target));
    reports.triage(
        id,
        moderator,
        "triage",
        0,
        new ReportService.Triage(ReportService.Action.TRIAGE, "Needs verification"),
        "test");
    assertThrows(
        ApiException.class,
        () ->
            reports.triage(
                id,
                moderator,
                "resolve",
                1,
                new ReportService.Triage(ReportService.Action.RESOLVE, "Verified"),
                "test"));
    var senior = new ReviewRules.Actor(UUID.randomUUID(), Set.of("SENIOR_MODERATOR"));
    var resolved =
        reports.triage(
            id,
            senior,
            "senior-resolve",
            1,
            new ReportService.Triage(
                ReportService.Action.RESOLVE,
                "Verified report; Catalog visibility remains externally managed"),
            "test");
    assertEquals("RESOLVED", resolved.get("state"));
    assertEquals(
        0,
        jdbc.queryForObject(
            "select count(*) from outbox where aggregate_id=? and topic='submission.workflow.v1'",
            Integer.class,
            id));
    assertEquals(
        3,
        jdbc.queryForObject("select count(*) from outbox where aggregate_id=?", Integer.class, id));
  }
}
