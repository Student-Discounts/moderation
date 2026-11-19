package org.studentdiscounts.media;

import static org.junit.jupiter.api.Assertions.*;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.*;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.*;
import com.sun.net.httpserver.HttpServer;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import javax.imageio.ImageIO;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MediaRuntimeIT {
  @Container static final PostgreSQLContainer pg = new PostgreSQLContainer("postgres:17-bookworm");
  @Container static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.1.0");

  @Container
  static final GenericContainer<?> minio =
      new GenericContainer<>("pgsty/silo:RELEASE.2026-11-12T00-00-00Z")
          .withEnv("MINIO_ROOT_USER", "media_test")
          .withEnv("MINIO_ROOT_PASSWORD", "media_test_secret")
          .withExposedPorts(9000)
          .withCommand("server /data")
          .waitingFor(
              org.testcontainers.containers.wait.strategy.Wait.forHttp("/minio/health/ready")
                  .forPort(9000));

  static final String SERVICE_TOKEN = "test-only-media-service-credential-over-32-characters";
  static final RSAKey SIGNING_KEY;
  static final HttpServer JWKS;

  static {
    try {
      SIGNING_KEY = new RSAKeyGenerator(2048).keyID("media-runtime-test").generate();
      JWKS = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      JWKS.createContext(
          "/jwks",
          exchange -> {
            byte[] body =
                new JWKSet(SIGNING_KEY.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) {
              out.write(body);
            }
          });
      JWKS.start();
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry p) {
    p.add("spring.datasource.url", pg::getJdbcUrl);
    p.add("spring.datasource.username", pg::getUsername);
    p.add("spring.datasource.password", pg::getPassword);
    p.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    p.add("media.service-token", () -> SERVICE_TOKEN);
    p.add("media.issuer", () -> "https://issuer.media.test");
    p.add("media.audience", () -> "student-discounts-api");
    p.add("media.jwks-uri", () -> "http://127.0.0.1:" + JWKS.getAddress().getPort() + "/jwks");
    p.add(
        "media.storage.endpoint",
        () -> "http://" + minio.getHost() + ":" + minio.getMappedPort(9000));
    p.add("media.storage.access-key", () -> "media_test");
    p.add("media.storage.secret-key", () -> "media_test_secret");
    p.add("media.storage.create-bucket", () -> true);
    p.add("media.workers-enabled", () -> false);
    p.add("media.outbox-enabled", () -> false);
  }

  @Value("${local.server.port}")
  int port;

  @Autowired JdbcTemplate jdbc;
  @Autowired MediaService service;
  @Autowired MediaOutbox outbox;
  @Autowired PrivateStorage storage;
  @Autowired JsonMapper json;
  final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
  final UUID owner = UUID.randomUUID();

  @AfterAll
  static void stopJwks() {
    JWKS.stop(0);
  }

  @BeforeEach
  void clear() {
    jdbc.execute(
        "truncate upload_idempotency,media_audit,media_outbox,media_asset,media_rate_window");
  }

  String token(
      UUID sub, List<String> roles, String issuer, String audience, Instant expires, RSAKey key)
      throws Exception {
    var claims =
        new JWTClaimsSet.Builder()
            .subject(sub.toString())
            .issuer(issuer)
            .audience(audience)
            .issueTime(Date.from(Instant.now()))
            .expirationTime(Date.from(expires))
            .claim("roles", roles)
            .build();
    var jwt =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(SIGNING_KEY.getKeyID()).build(),
            claims);
    jwt.sign(new RSASSASigner(key));
    return jwt.serialize();
  }

  String token(UUID sub, String role) throws Exception {
    return token(
        sub,
        List.of(role),
        "https://issuer.media.test",
        "student-discounts-api",
        Instant.now().plusSeconds(120),
        SIGNING_KEY);
  }

  HttpResponse<byte[]> get(String path, String bearer, String serviceCredential) throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .timeout(Duration.ofSeconds(10));
    if (bearer != null) request.header("Authorization", "Bearer " + bearer);
    if (serviceCredential != null) request.header("X-Service-Token", serviceCredential);
    return http.send(request.GET().build(), HttpResponse.BodyHandlers.ofByteArray());
  }

  byte[] image() throws Exception {
    var out = new ByteArrayOutputStream();
    ImageIO.write(new BufferedImage(3, 2, BufferedImage.TYPE_INT_RGB), "png", out);
    return out.toByteArray();
  }

  HttpResponse<byte[]> upload(byte[] file, String name, String mime, String key) throws Exception {
    String boundary = "media-runtime-boundary";
    var body = new ByteArrayOutputStream();
    body.write(
        ("--"
                + boundary
                + "\r\nContent-Disposition: form-data; name=\"file\"; filename=\""
                + name
                + "\"\r\nContent-Type: "
                + mime
                + "\r\n\r\n")
            .getBytes(StandardCharsets.UTF_8));
    body.write(file);
    body.write(("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
    var request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/media-assets"))
            .header("Authorization", "Bearer " + token(owner, "STUDENT"))
            .header("Idempotency-Key", key)
            .header("Content-Type", "multipart/form-data; boundary=" + boundary)
            .timeout(Duration.ofSeconds(30))
            .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()))
            .build();
    return http.send(request, HttpResponse.BodyHandlers.ofByteArray());
  }

  UUID uploaded(byte[] image, String name, String mime, String key) throws Exception {
    var response = upload(image, name, mime, key);
    assertEquals(202, response.statusCode(), new String(response.body()));
    return UUID.fromString(json.readTree(response.body()).path("id").asText());
  }

  @Test
  void realSignedJwtOwnershipSafePreviewAndIdempotency() throws Exception {
    byte[] png = image();
    UUID id = uploaded(png, "proof.png", "image/png", "same");
    assertEquals(
        0,
        json.readTree(get("/api/v1/media-assets/" + id, token(owner, "STUDENT"), null).body())
            .path("revision")
            .asInt());
    assertEquals(
        409,
        get("/api/v1/media-assets/" + id + "/preview", token(owner, "STUDENT"), null).statusCode());
    assertEquals(id, uploaded(png, "proof.png", "image/png", "same"));
    assertEquals(409, upload(png, "changed.png", "image/png", "same").statusCode());
    assertEquals(
        409,
        upload(Arrays.copyOf(png, png.length + 1), "proof.png", "image/png", "same").statusCode());
    assertEquals(409, upload(png, "proof.png", "image/jpeg", "same").statusCode());
    assertEquals(1, jdbc.queryForObject("select count(*) from media_asset", Integer.class));
    process();
    assertEquals("CLEAN", service.find(id).status());
    assertEquals(2, service.find(id).revision());
    String path = "/api/v1/media-assets/" + id;
    assertEquals(404, get(path, token(UUID.randomUUID(), "STUDENT"), null).statusCode());
    assertEquals(
        404, get(path + "/preview", token(UUID.randomUUID(), "STUDENT"), null).statusCode());
    assertEquals(200, get(path, token(UUID.randomUUID(), "MODERATOR"), null).statusCode());
    var preview = get(path + "/preview", token(owner, "STUDENT"), null);
    assertEquals(200, preview.statusCode());
    assertEquals("image/png", preview.headers().firstValue("Content-Type").orElseThrow());
    assertTrue(preview.headers().firstValue("Cache-Control").orElseThrow().contains("no-store"));
    assertEquals("nosniff", preview.headers().firstValue("X-Content-Type-Options").orElseThrow());
    assertNotNull(ImageIO.read(new java.io.ByteArrayInputStream(preview.body())));
    assertFalse(
        new String(get(path, token(owner, "STUDENT"), null).body()).contains("quarantine/"));
    assertEquals(3, jdbc.queryForObject("select count(*) from media_audit", Integer.class));
  }

  @Test
  void rejectsInvalidSignaturesIssuerAudienceExpiryAndMissingRoles() throws Exception {
    String path = "/api/v1/media-assets/" + UUID.randomUUID();
    assertEquals(401, get(path, null, null).statusCode());
    RSAKey wrong = new RSAKeyGenerator(2048).generate();
    for (String jwt :
        List.of(
            token(
                owner,
                List.of("STUDENT"),
                "https://issuer.media.test",
                "student-discounts-api",
                Instant.now().plusSeconds(60),
                wrong),
            token(
                owner,
                List.of("STUDENT"),
                "wrong",
                "student-discounts-api",
                Instant.now().plusSeconds(60),
                SIGNING_KEY),
            token(
                owner,
                List.of("STUDENT"),
                "https://issuer.media.test",
                "wrong",
                Instant.now().plusSeconds(60),
                SIGNING_KEY),
            token(
                owner,
                List.of("STUDENT"),
                "https://issuer.media.test",
                "student-discounts-api",
                Instant.now().minusSeconds(120),
                SIGNING_KEY),
            token(
                owner,
                List.of(),
                "https://issuer.media.test",
                "student-discounts-api",
                Instant.now().plusSeconds(60),
                SIGNING_KEY))) assertEquals(401, get(path, jwt, null).statusCode());
  }

  @Test
  void consumerContractRequiresDedicatedTokenNotUserJwt() throws Exception {
    UUID id = uploaded(image(), "proof.png", "image/png", "consumer");
    String path = "/media/v1/assets/" + id;
    assertEquals(401, get(path, null, null).statusCode());
    assertEquals(401, get(path, token(owner, "STUDENT"), null).statusCode());
    assertEquals(401, get(path, null, SERVICE_TOKEN + "x").statusCode());
    var response = get(path, null, SERVICE_TOKEN);
    assertEquals(200, response.statusCode());
    var metadata = json.readTree(response.body());
    assertEquals(4, metadata.size());
    assertEquals(owner.toString(), metadata.path("ownerId").asText());
    assertEquals(id.toString(), metadata.path("id").asText());
  }

  @Test
  void rejectsDuplicatedServiceCredentialAndAnonymousS3Read() throws Exception {
    UUID id = uploaded(image(), "private.png", "image/png", "private");
    var request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/media/v1/assets/" + id))
            .header("X-Service-Token", SERVICE_TOKEN)
            .header("X-Service-Token", SERVICE_TOKEN)
            .GET()
            .build();
    assertEquals(401, http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode());
    String object = service.find(id).originalKey();
    var anonymous =
        HttpRequest.newBuilder(
                URI.create(
                    "http://"
                        + minio.getHost()
                        + ":"
                        + minio.getMappedPort(9000)
                        + "/media-private/"
                        + object))
            .GET()
            .build();
    assertEquals(403, http.send(anonymous, HttpResponse.BodyHandlers.discarding()).statusCode());
  }

  @Test
  void actualUploadDatabaseFailureRollsBackMetadataIdempotencyAuditAndOutbox() throws Exception {
    jdbc.execute(
        "create function reject_media_test_insert() returns trigger language plpgsql as $$ begin"
            + " raise exception 'test database failure'; end $$");
    jdbc.execute(
        "create trigger reject_media_test before insert on media_asset for each row execute"
            + " function reject_media_test_insert()");
    try {
      var result = upload(image(), "rollback.png", "image/png", "rollback");
      assertEquals(503, result.statusCode());
      for (String table :
          List.of("media_asset", "media_outbox", "media_audit", "upload_idempotency"))
        assertEquals(0, jdbc.queryForObject("select count(*) from " + table, Integer.class));
    } finally {
      jdbc.execute("drop trigger reject_media_test on media_asset");
      jdbc.execute("drop function reject_media_test_insert()");
    }
    assertNotNull(uploaded(image(), "rollback.png", "image/png", "rollback"));
  }

  @Test
  void uploadByteAndPixelLimitsAndImmutableAudit() throws Exception {
    assertEquals(
        413,
        upload(new byte[10 * 1024 * 1024 + 1], "large.png", "image/png", "large").statusCode());
    byte[] bomb = image();
    java.nio.ByteBuffer.wrap(bomb).putInt(16, 100000).putInt(20, 100000);
    UUID id = uploaded(bomb, "bomb.png", "image/png", "bomb");
    process();
    assertEquals("REJECTED", service.find(id).status());
    assertThrows(
        org.springframework.dao.DataAccessException.class,
        () -> jdbc.update("delete from media_audit where asset_id=?", id));
  }

  @Test
  void databaseRateAndUploadQuotaWindowsReturnRetryAfterWithoutWritingAssets() throws Exception {
    jdbc.update(
        "insert into media_rate_window(actor_id,window_start,kind,count)"
            + " values(?,date_trunc('day',now() at time zone 'UTC') at time zone 'UTC','day',?)",
        owner,
        properties.limits().uploadsPerDay());
    var quota = upload(image(), "proof.png", "image/png", "quota");
    assertEquals(429, quota.statusCode());
    assertTrue(quota.headers().firstValue("Retry-After").isPresent());
    assertEquals(0, jdbc.queryForObject("select count(*) from media_asset", Integer.class));
    jdbc.update(
        "insert into media_rate_window(actor_id,window_start,kind,count)"
            + " values(?,date_trunc('minute',now() at time zone 'UTC') at time zone"
            + " 'UTC','minute',?) on conflict(actor_id,window_start,kind) do update set"
            + " count=excluded.count",
        owner,
        properties.limits().requestsPerMinute());
    var limited = get("/api/v1/media-assets/" + UUID.randomUUID(), token(owner, "STUDENT"), null);
    assertEquals(429, limited.statusCode());
    assertTrue(limited.headers().firstValue("Retry-After").isPresent());
  }

  @Test
  void corruptAndSpoofedTypesRejectButStorageFailuresRemainRetryable() throws Exception {
    UUID spoof = uploaded(image(), "proof.jpg", "image/jpeg", "spoof");
    process();
    assertEquals("REJECTED", service.find(spoof).status());
    assertNull(service.find(spoof).cleanKey());
    UUID corrupt = uploaded(Arrays.copyOf(image(), 32), "proof.png", "image/png", "corrupt");
    process();
    assertEquals("REJECTED", service.find(corrupt).status());
    UUID missing = uploaded(image(), "proof.png", "image/png", "missing");
    storage.delete(service.find(missing).originalKey());
    process();
    assertEquals("PROCESSING", service.find(missing).status());
    assertNull(service.find(missing).cleanKey());
    // Restore the real object, expire the retry lease, and prove a restarted worker can recover it.
    storage.put(service.find(missing).originalKey(), image(), "application/octet-stream");
    jdbc.update(
        "update media_asset set lease_until=now()-interval '1 minute',next_attempt_at=now() where"
            + " id=?",
        missing);
    new MediaWorker(service, storage, applicationProperties()).processNext();
    assertEquals("CLEAN", service.find(missing).status());
  }

  void process() {
    new MediaWorker(service, storage, applicationProperties()).processNext();
  }

  @Autowired MediaProperties properties;

  MediaProperties applicationProperties() {
    return new MediaProperties(
        properties.serviceToken(),
        properties.issuer(),
        properties.audience(),
        properties.jwksUri(),
        properties.storage(),
        properties.limits(),
        true,
        false,
        properties.workflowTopic());
  }

  @Test
  void outboxKafkaUsesStableEventIdsAndPreservesAssetRevisionOrderOnReplay() throws Exception {
    UUID id = uploaded(image(), "proof.png", "image/png", "events");
    process();
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "media-test-" + UUID.randomUUID());
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    try (var consumer = new KafkaConsumer<String, String>(config)) {
      consumer.subscribe(List.of(properties.workflowTopic()));
      assertTrue(outbox.publishOne());
      assertTrue(outbox.publishOne());
      assertTrue(outbox.publishOne());
      assertFalse(outbox.publishOne());
      var events =
          new ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>>();
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (events.size() < 3 && System.nanoTime() < deadline)
        consumer
            .poll(Duration.ofMillis(500))
            .forEach(r -> {
              if (r.key().equals(id.toString())) events.add(r);
            });
      assertEquals(3, events.size());
      for (int i = 0; i < 3; i++) {
        var event = json.readTree(events.get(i).value());
        assertEquals(i, event.path("data").path("revision").asInt());
        assertEquals("MediaAssetChecked", event.path("eventType").asText());
        assertEquals("media", event.path("producer").asText());
      }
      String firstEvent = json.readTree(events.getFirst().value()).path("eventId").asText();
      jdbc.update(
          "update media_outbox set published_at=null where event_id=?",
          UUID.fromString(firstEvent));
      assertTrue(outbox.publishOne());
      String replay = null;
      deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      while (replay == null && System.nanoTime() < deadline)
        for (var record : consumer.poll(Duration.ofMillis(500)))
          if (record.key().equals(id.toString()))
            replay = json.readTree(record.value()).path("eventId").asText();
      assertEquals(firstEvent, replay);
    }
  }
}
