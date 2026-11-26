package org.studentdiscounts.moderation;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.*;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

@Testcontainers
@SpringBootTest(
    properties = {
      "moderation.listener-enabled=true",
      "moderation.outbox-enabled=true",
      "moderation.local-topics-enabled=true"
    })
class KafkaWorkflowTest {
  @Container
  static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

  @Container static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.1.0");

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry p) {
    p.add("spring.datasource.url", postgres::getJdbcUrl);
    p.add("spring.datasource.username", postgres::getUsername);
    p.add("spring.datasource.password", postgres::getPassword);
    p.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    p.add("spring.kafka.consumer.group-id", () -> "moderation-kafka-test");
    p.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> "http://localhost:18080");
    p.add(
        "spring.security.oauth2.resourceserver.jwt.jwk-set-uri",
        () -> "http://localhost:18080/jwks");
  }

  @Autowired ReviewService reviews;
  @Autowired JdbcTemplate jdbc;
  @Autowired ObjectMapper json;
  @Autowired KafkaTemplate<String, String> producer;

  ObjectNode event(
      UUID id,
      UUID version,
      UUID author,
      int no,
      long sequence,
      String type,
      String status,
      long row) {
    var e =
        json.createObjectNode()
            .put("eventId", UUID.randomUUID().toString())
            .put("eventType", type)
            .put("schemaVersion", 1)
            .put("producer", "submission")
            .put("occurredAt", "2026-11-19T12:00:00Z")
            .put("correlationId", "kafka-test")
            .put("aggregateSequence", sequence);
    var d =
        e.putObject("data")
            .put("submissionId", id.toString())
            .put("versionId", version.toString())
            .put("versionNo", no)
            .put("rowVersion", row);
    if (type.equals("SubmissionStatusChanged"))
      d.put("fromStatus", "VALIDATING").put("toStatus", status);
    else {
      d.put("authorId", author.toString())
          .put("payloadSchemaVersion", 1)
          .put("contentSha256", "b".repeat(64))
          .putNull("baseOfferRowVersion");
      d.putObject("payload").put("place", "immutable-test");
    }
    return e;
  }

  void send(UUID aggregate, ObjectNode event) throws Exception {
    producer
        .send("submission.events.v1", aggregate.toString(), event.toString())
        .get(10, java.util.concurrent.TimeUnit.SECONDS);
  }

  static void eventually(BooleanSupplier condition) throws Exception {
    long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (!condition.getAsBoolean() && System.nanoTime() < until) Thread.sleep(25);
    assertTrue(condition.getAsBoolean(), "condition not reached");
  }

  Consumer<String, String> consumer(String topic) {
    var consumer =
        new KafkaConsumer<String, String>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                UUID.randomUUID().toString(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false),
            new StringDeserializer(),
            new StringDeserializer());
    consumer.subscribe(List.of(topic));
    return consumer;
  }

  String find(Consumer<String, String> consumer, UUID eventId) throws Exception {
    long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    do {
      for (var r : consumer.poll(Duration.ofMillis(250)))
        if (json.readTree(r.value()).path("eventId").asString().equals(eventId.toString()))
          return r.value();
    } while (System.nanoTime() < until);
    fail("Expected stable eventId not delivered");
    return null;
  }

  @Test
  void actualBrokerRelayClaimConfirmationAndDltRejectionAreNotOptimistic() throws Exception {
    UUID id = UUID.randomUUID(),
        v = UUID.randomUUID(),
        author = UUID.randomUUID(),
        actor = UUID.randomUUID();
    var moderator = new ReviewRules.Actor(actor, Set.of("MODERATOR"));
    send(id, event(id, v, author, 1, 1, "SubmissionSubmitted", null, 1));
    send(id, event(id, v, author, 1, 2, "SubmissionStatusChanged", "PENDING_REVIEW", 3));
    eventually(() ->
        jdbc.queryForObject(
                "select count(*) from reviews where submission_id=? and"
                    + " submission_status='PENDING_REVIEW'",
                Integer.class,
                id)
            == 1);
    try (var output = consumer("submission.workflow.v1")) {
      long etag = ((Number) reviews.detail(id).get("localVersion")).longValue();
      var claimed =
          reviews.command(
              id,
              moderator,
              "kafka-claim",
              etag,
              new ReviewService.Command(ReviewRules.Action.CLAIM, v, 1, null),
              "kafka-test");
      UUID claimId = UUID.fromString(claimed.get("commandId").toString());
      var claim = json.readTree(find(output, claimId));
      assertEquals("ReviewStarted", claim.path("eventType").asString());
      assertEquals(
          Set.of(
              "eventId",
              "eventType",
              "schemaVersion",
              "producer",
              "occurredAt",
              "correlationId",
              "data"),
          claim.propertyNames());
      assertEquals(3, claim.path("data").path("expectedRowVersion").asInt());
      assertEquals("PENDING_REVIEW", reviews.detail(id).get("status"));
      // Simulate the allowed crash window after broker acknowledgement but before marking
      // published.
      jdbc.update("update outbox set published_at=null where id=?", claimId);
      assertEquals(claim, json.readTree(find(output, claimId)));
      var confirmation = event(id, v, author, 1, 3, "SubmissionStatusChanged", "IN_REVIEW", 4);
      ((ObjectNode) confirmation.get("data"))
          .put("actorId", actor.toString())
          .put("expectedRowVersion", 4);
      send(id, confirmation);
      send(id, confirmation);
      eventually(() ->
          "CONFIRMED"
              .equals(
                  jdbc.queryForObject(
                      "select state from commands where id=?", String.class, claimId)));
      etag = ((Number) reviews.detail(id).get("localVersion")).longValue();
      var approved =
          reviews.command(
              id,
              moderator,
              "kafka-approve",
              etag,
              new ReviewService.Command(ReviewRules.Action.APPROVE, v, 1, null),
              "kafka-test");
      UUID approvalId = UUID.fromString(approved.get("commandId").toString());
      String command = find(output, approvalId);
      assertEquals("SubmissionApproved", json.readTree(command).path("eventType").asString());
      assertEquals(
          json.readTree(command).path("data").path("payload"),
          reviews.snapshot(id, 1).get("payload"));
      assertEquals("IN_REVIEW", reviews.detail(id).get("status"));
      producer
          .send("submission.workflow.v1.DLT", id.toString(), command)
          .get(10, java.util.concurrent.TimeUnit.SECONDS);
      eventually(() ->
          "REJECTED"
              .equals(
                  jdbc.queryForObject(
                      "select state from commands where id=?", String.class, approvalId)));
      assertNull(reviews.detail(id).get("pendingCommandId"));
      assertEquals("IN_REVIEW", reviews.detail(id).get("status"));
      assertEquals(
          1,
          jdbc.queryForObject(
              "select count(*) from decisions where command_id=?", Integer.class, claimId));
      assertEquals(
          3,
          jdbc.queryForObject(
              "select count(*) from inbox where aggregate_id=? and source='submission.events.v1'",
              Integer.class,
              id));
    }
  }

  @Test
  void poisonIsDurablyVisibleAndPublishedToDltWithoutInboxMutation() throws Exception {
    UUID id = UUID.randomUUID(), eventId = UUID.randomUUID();
    var poison =
        event(id, UUID.randomUUID(), UUID.randomUUID(), 1, 1, "SubmissionSubmitted", null, 1)
            .put("eventId", eventId.toString())
            .put("producer", "untrusted");
    try (var dlt = consumer("moderation.consumer.v1.DLT")) {
      send(id, poison);
      assertEquals(poison.toString(), find(dlt, eventId));
      eventually(() ->
          jdbc.queryForObject(
                  "select count(*) from consumer_failures where event_id=?", Integer.class, eventId)
              == 1);
      assertEquals(
          0,
          jdbc.queryForObject(
              "select count(*) from inbox where event_id=?", Integer.class, eventId));
    }
  }

  @Test
  void failedAggregateDoesNotOvertakeOrStarveOtherAggregate() throws Exception {
    UUID blocked = UUID.randomUUID(),
        independent = UUID.randomUUID(),
        one = UUID.randomUUID(),
        two = UUID.randomUUID(),
        three = UUID.randomUUID();
    String payload = json.createObjectNode().put("eventId", three.toString()).toString();
    jdbc.update(
        "insert into outbox(id,aggregate_id,aggregate_sequence,topic,payload) values(?,?,1,'invalid"
            + " topic',?::jsonb)",
        one,
        blocked,
        payload);
    jdbc.update(
        "insert into outbox(id,aggregate_id,aggregate_sequence,topic,payload)"
            + " values(?,?,2,'moderation.events.v1',?::jsonb)",
        two,
        blocked,
        payload);
    jdbc.update(
        "insert into outbox(id,aggregate_id,aggregate_sequence,topic,payload)"
            + " values(?,?,1,'moderation.events.v1',?::jsonb)",
        three,
        independent,
        payload);
    eventually(() ->
        jdbc.queryForObject(
            "select published_at is not null from outbox where id=?", Boolean.class, three));
    assertFalse(
        jdbc.queryForObject(
            "select published_at is not null from outbox where id=?", Boolean.class, one));
    assertFalse(
        jdbc.queryForObject(
            "select published_at is not null from outbox where id=?", Boolean.class, two));
    assertTrue(
        jdbc.queryForObject("select failure_count>0 from outbox where id=?", Boolean.class, one));
  }
}
