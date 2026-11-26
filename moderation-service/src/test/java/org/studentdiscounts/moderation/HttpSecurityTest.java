package org.studentdiscounts.moderation;

import static org.junit.jupiter.api.Assertions.*;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.*;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "moderation.listener-enabled=false",
      "moderation.outbox-enabled=false",
      "moderation.local-topics-enabled=false",
      "moderation.requests-per-minute=20"
    })
class HttpSecurityTest {
  @Container
  static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

  @Container static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.1.0");
  static final RSAKey key;
  static final HttpServer jwks;
  static final String issuer;

  static {
    try {
      key = new RSAKeyGenerator(2048).keyID("http-test").generate();
      jwks = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      jwks.createContext(
          "/jwks",
          exchange -> {
            byte[] body = new JWKSet(key.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var stream = exchange.getResponseBody()) {
              stream.write(body);
            }
          });
      jwks.start();
      issuer = "http://127.0.0.1:" + jwks.getAddress().getPort();
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @AfterAll
  static void close() {
    jwks.stop(0);
  }

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry p) {
    p.add("spring.datasource.url", postgres::getJdbcUrl);
    p.add("spring.datasource.username", postgres::getUsername);
    p.add("spring.datasource.password", postgres::getPassword);
    p.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    p.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> issuer);
    p.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri", () -> issuer + "/jwks");
  }

  @Value("${local.server.port}")
  int port;

  final HttpClient client = HttpClient.newHttpClient();

  JWTClaimsSet.Builder claims(UUID actor, String role) {
    return new JWTClaimsSet.Builder()
        .issuer(issuer)
        .subject(actor.toString())
        .audience("student-discounts-api")
        .claim("roles", List.of(role))
        .expirationTime(Date.from(Instant.now().plusSeconds(300)));
  }

  String sign(JWTClaimsSet claims, RSAKey signer) throws Exception {
    SignedJWT token =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(), claims);
    token.sign(new RSASSASigner(signer));
    return token.serialize();
  }

  HttpResponse<String> request(
      String method, String path, String token, String body, String... headers) throws Exception {
    var r = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path));
    if (token != null) r.header("Authorization", "Bearer " + token);
    for (int i = 0; i < headers.length; i += 2) r.header(headers[i], headers[i + 1]);
    if (body != null) r.header("Content-Type", "application/json");
    return client.send(
        r.method(
                method,
                body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  @Test
  void signatureIssuerAudienceExpirySubjectAndRoleShapeAreActuallyChecked() throws Exception {
    UUID id = UUID.randomUUID();
    String path = "/api/v1/moderation/queue";
    assertEquals(401, request("GET", path, null, null).statusCode());
    for (JWTClaimsSet bad :
        List.of(
            claims(id, "MODERATOR").issuer("https://wrong.example").build(),
            claims(id, "MODERATOR").audience("wrong").build(),
            claims(id, "MODERATOR").audience((String) null).build(),
            claims(id, "MODERATOR").expirationTime(null).build(),
            claims(id, "MODERATOR")
                .expirationTime(Date.from(Instant.now().minusSeconds(120)))
                .build(),
            claims(id, "MODERATOR").subject("1-1-1-1-1").build(),
            claims(id, "MODERATOR").claim("roles", "MODERATOR").build())) {
      assertEquals(401, request("GET", path, sign(bad, key), null).statusCode());
    }
    assertEquals(
        401,
        request(
                "GET",
                path,
                sign(claims(id, "MODERATOR").build(), new RSAKeyGenerator(2048).generate()),
                null)
            .statusCode());
    assertEquals(
        403, request("GET", path, sign(claims(id, "STUDENT").build(), key), null).statusCode());
    assertEquals(
        200, request("GET", path, sign(claims(id, "MODERATOR").build(), key), null).statusCode());
  }

  @Test
  void studentsSeeOnlyTheirReportsAndReportsNeverHideAnything() throws Exception {
    UUID owner = UUID.randomUUID();
    String token = sign(claims(owner, "STUDENT").build(), key);
    String body =
        "{\"submissionId\":\""
            + UUID.randomUUID()
            + "\",\"category\":\"OUTDATED\",\"description\":\"Discount no longer available\"}";
    var created =
        request(
            "POST",
            "/api/v1/reports",
            token,
            body,
            "Idempotency-Key",
            "report-test",
            "X-Correlation-ID",
            "http-test");
    assertEquals(201, created.statusCode(), created.body());
    assertEquals("http-test", created.headers().firstValue("X-Correlation-ID").orElseThrow());
    String id = JsonMapper.builder().build().readTree(created.body()).path("id").asString();
    assertEquals(200, request("GET", "/api/v1/reports/" + id, token, null).statusCode());
    assertEquals(
        404,
        request(
                "GET",
                "/api/v1/reports/" + id,
                sign(claims(UUID.randomUUID(), "STUDENT").build(), key),
                null)
            .statusCode());
    assertEquals(
        201,
        request("POST", "/api/v1/reports", token, body, "Idempotency-Key", "report-test")
            .statusCode());
    assertEquals(
        "OPEN", JsonMapper.builder().build().readTree(created.body()).path("state").asString());
  }

  @Test
  void boundedBodyPaginationAndRateReturnProblems() throws Exception {
    String token = sign(claims(UUID.randomUUID(), "MODERATOR").build(), key);
    var bad = request("GET", "/api/v1/moderation/queue?limit=101", token, null);
    assertEquals(400, bad.statusCode());
    assertTrue(
        bad.headers().firstValue("Content-Type").orElse("").contains("application/problem+json"));
    String student = sign(claims(UUID.randomUUID(), "STUDENT").build(), key);
    assertEquals(
        413,
        request(
                "POST",
                "/api/v1/reports",
                student,
                "{\"description\":\"" + "x".repeat(270000) + "\"}",
                "Idempotency-Key",
                "large")
            .statusCode());
    assertEquals(
        413,
        chunkedReport(student, "large-chunked", "{\"description\":\"" + "x".repeat(270000) + "\"}")
            .statusCode());
    UUID target = UUID.randomUUID();
    var accepted =
        chunkedReport(
            student,
            "small-chunked",
            "{\"submissionId\":\""
                + target
                + "\",\"category\":\"OUTDATED\",\"description\":\"Check availability\"}");
    assertEquals(201, accepted.statusCode());
    assertEquals(
        target.toString(),
        JsonMapper.builder().build().readTree(accepted.body()).path("submissionId").asString());
    HttpResponse<String> limited = null;
    for (int i = 0; i < 21; i++) limited = request("GET", "/api/v1/reports", student, null);
    assertEquals(429, limited.statusCode());
    assertTrue(limited.headers().firstValue("Retry-After").isPresent());
  }

  private HttpResponse<String> chunkedReport(String token, String key, String body)
      throws Exception {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    var request =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/v1/reports"))
            .header("Authorization", "Bearer " + token)
            .header("Content-Type", "application/json")
            .header("Idempotency-Key", key)
            .POST(
                HttpRequest.BodyPublishers.ofInputStream(
                    () -> new java.io.ByteArrayInputStream(bytes)))
            .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
