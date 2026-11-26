package org.studentdiscounts.media;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.*;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.web.filter.OncePerRequestFilter;

@Configuration
public class MediaSecurity {
  private static final Set<String> ROLES =
      Set.of("STUDENT", "ORG_REPRESENTATIVE", "MODERATOR", "SENIOR_MODERATOR");

  static boolean identityValid(Jwt jwt) {
    try {
      String sub = jwt.getSubject();
      if (sub == null || !UUID.fromString(sub).toString().equals(sub) || jwt.getExpiresAt() == null)
        return false;
      Object roles = jwt.getClaims().get("roles");
      return roles instanceof Collection<?> values
          && values.stream().allMatch(String.class::isInstance)
          && values.stream().anyMatch(ROLES::contains);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  static boolean canRead(UUID owner, Jwt jwt) {
    if (!identityValid(jwt)) return false;
    Collection<?> roles = (Collection<?>) jwt.getClaims().get("roles");
    return owner.toString().equals(jwt.getSubject())
        || roles.contains("MODERATOR")
        || roles.contains("SENIOR_MODERATOR");
  }

  static boolean serviceTokenMatches(String expected, String actual) {
    if (expected == null
        || !expected.matches("[\\x21-\\x7e]{32,256}")
        || actual == null
        || !actual.matches("[\\x21-\\x7e]{32,256}")) return false;
    return MessageDigest.isEqual(digest(expected), digest(actual));
  }

  private static byte[] digest(String value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  @Bean
  JwtDecoder jwtDecoder(MediaProperties properties) {
    NimbusJwtDecoder decoder =
        NimbusJwtDecoder.withJwkSetUri(properties.jwksUri().toString())
            .jwsAlgorithm(SignatureAlgorithm.RS256)
            .build();
    OAuth2TokenValidator<Jwt> audience =
        jwt ->
            jwt.getAudience().contains(properties.audience())
                ? OAuth2TokenValidatorResult.success()
                : invalid();
    OAuth2TokenValidator<Jwt> identity =
        jwt -> identityValid(jwt) ? OAuth2TokenValidatorResult.success() : invalid();
    decoder.setJwtValidator(
        new DelegatingOAuth2TokenValidator<>(
            JwtValidators.createDefaultWithIssuer(properties.issuer()), audience, identity));
    return decoder;
  }

  private static OAuth2TokenValidatorResult invalid() {
    return OAuth2TokenValidatorResult.failure(
        new OAuth2Error("invalid_token", "Invalid access token", null));
  }

  @Bean
  @Order(1)
  SecurityFilterChain serviceChain(HttpSecurity http, MediaProperties properties) throws Exception {
    http.securityMatcher("/media/**")
        .csrf(c -> c.disable())
        .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .addFilterBefore(
            new OncePerRequestFilter() {
              @Override
              protected void doFilterInternal(
                  HttpServletRequest request, HttpServletResponse response, FilterChain chain)
                  throws IOException, ServletException {
                if (java.util.Collections.list(request.getHeaders("X-Service-Token")).size() != 1
                    || !serviceTokenMatches(
                        properties.serviceToken(), request.getHeader("X-Service-Token"))) {
                  securityProblem(response, 401, "SERVICE_CREDENTIAL_REQUIRED");
                  return;
                }
                chain.doFilter(request, response);
              }
            },
            AnonymousAuthenticationFilter.class)
        .authorizeHttpRequests(a ->
            a.requestMatchers(HttpMethod.GET, "/media/v1/assets/*")
                .permitAll()
                .anyRequest()
                .denyAll());
    return http.build();
  }

  @Bean
  @Order(2)
  SecurityFilterChain userChain(HttpSecurity http) throws Exception {
    var roles = new JwtGrantedAuthoritiesConverter();
    roles.setAuthoritiesClaimName("roles");
    roles.setAuthorityPrefix("ROLE_");
    var converter = new JwtAuthenticationConverter();
    converter.setJwtGrantedAuthoritiesConverter(roles);
    http.csrf(c -> c.disable())
        .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .authorizeHttpRequests(a ->
            a.requestMatchers("/actuator/health/**", "/openapi.yaml")
                .permitAll()
                .requestMatchers("/api/v1/media-assets/**")
                .hasAnyRole("STUDENT", "ORG_REPRESENTATIVE", "MODERATOR", "SENIOR_MODERATOR")
                .anyRequest()
                .denyAll())
        .exceptionHandling(e ->
            e.authenticationEntryPoint(
                    (req, res, ex) -> securityProblem(res, 401, "AUTHENTICATION_REQUIRED"))
                .accessDeniedHandler((req, res, ex) -> securityProblem(res, 403, "ACCESS_DENIED")))
        .oauth2ResourceServer(o ->
            o.jwt(j -> j.jwtAuthenticationConverter(converter))
                .authenticationEntryPoint(
                    (req, res, ex) -> securityProblem(res, 401, "INVALID_ACCESS_TOKEN")));
    return http.build();
  }

  static void securityProblem(HttpServletResponse response, int status, String code)
      throws IOException {
    response.setStatus(status);
    response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
    response.setHeader("Cache-Control", "no-store");
    response
        .getWriter()
        .write(
            "{\"type\":\"about:blank\",\"status\":"
                + status
                + ",\"title\":\"Request denied\",\"code\":\""
                + code
                + "\"}");
  }
}
