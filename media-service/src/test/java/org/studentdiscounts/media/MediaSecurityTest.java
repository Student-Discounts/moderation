package org.studentdiscounts.media;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;

class MediaSecurityTest {
  private static Jwt token(String subject, Object roles) {
    return Jwt.withTokenValue("test")
        .header("alg", "RS256")
        .subject(subject)
        .claim("roles", roles)
        .issuedAt(Instant.now())
        .expiresAt(Instant.now().plusSeconds(60))
        .build();
  }

  @Test
  void requiresCanonicalUuidAndKnownRole() {
    String owner = UUID.randomUUID().toString();
    assertTrue(MediaSecurity.identityValid(token(owner, List.of("STUDENT"))));
    assertFalse(MediaSecurity.identityValid(token(owner.toUpperCase(), List.of("STUDENT"))));
    assertFalse(MediaSecurity.identityValid(token("1-1-1-1-1", List.of("STUDENT"))));
    assertFalse(MediaSecurity.identityValid(token(owner, "STUDENT")));
    assertFalse(MediaSecurity.identityValid(token(owner, List.of("ADMIN"))));
  }

  @Test
  void neverGrantsForeignAccessExceptReviewers() {
    UUID owner = UUID.randomUUID();
    UUID other = UUID.randomUUID();
    assertTrue(MediaSecurity.canRead(owner, token(owner.toString(), List.of("STUDENT"))));
    assertFalse(MediaSecurity.canRead(owner, token(other.toString(), List.of("STUDENT"))));
    assertFalse(
        MediaSecurity.canRead(owner, token(other.toString(), List.of("ORG_REPRESENTATIVE"))));
    assertTrue(MediaSecurity.canRead(owner, token(other.toString(), List.of("MODERATOR"))));
    assertTrue(MediaSecurity.canRead(owner, token(other.toString(), List.of("SENIOR_MODERATOR"))));
  }

  @Test
  void dedicatedCredentialFailsClosedAndDoesNotAcceptBearer() {
    String secret = "a-strong-private-service-token-at-least-32-characters";
    assertTrue(MediaSecurity.serviceTokenMatches(secret, secret));
    assertFalse(MediaSecurity.serviceTokenMatches(secret, null));
    assertFalse(MediaSecurity.serviceTokenMatches(secret, "Bearer " + secret));
    assertFalse(MediaSecurity.serviceTokenMatches(secret, secret + "x"));
    assertFalse(MediaSecurity.serviceTokenMatches("", ""));
  }
}
