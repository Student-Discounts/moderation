package org.studentdiscounts.moderation;

import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import org.springframework.context.annotation.*;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.*;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.oauth2.server.resource.web.authentication.BearerTokenAuthenticationFilter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.AuthorizationFilter;
import tools.jackson.databind.ObjectMapper;

@Configuration(proxyBeanMethods = false)
@EnableMethodSecurity
class SecurityConfiguration {
  @Bean
  OAuth2TokenValidator<Jwt> expirationRequired() {
    return new JwtClaimValidator<>("exp", Objects::nonNull);
  }

  @Bean
  OAuth2TokenValidator<Jwt> canonicalSubject() {
    return new JwtClaimValidator<Object>(
        "sub",
        value -> {
          if (!(value instanceof String s)) return false;
          try {
            return UUID.fromString(s).toString().equalsIgnoreCase(s);
          } catch (IllegalArgumentException e) {
            return false;
          }
        });
  }

  @Bean
  OAuth2TokenValidator<Jwt> rolesShape() {
    return jwt -> {
      Object value = jwt.getClaims().get("roles");
      if (!jwt.hasClaim("roles")
          || (value instanceof Collection<?> c && c.stream().allMatch(String.class::isInstance)))
        return OAuth2TokenValidatorResult.success();
      return OAuth2TokenValidatorResult.failure(
          new OAuth2Error("invalid_token", "roles must be a string array", null));
    };
  }

  @Bean
  SecurityFilterChain security(
      HttpSecurity http, ObjectMapper json, ReviewStore store, ModerationProperties properties)
      throws Exception {
    return http.csrf(c -> c.disable())
        .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .authorizeHttpRequests(a ->
            a.requestMatchers("/actuator/health", "/actuator/health/**", "/openapi.yaml")
                .permitAll()
                .requestMatchers("/actuator/**")
                .hasRole("SENIOR_MODERATOR")
                .requestMatchers("/api/v1/moderation/**")
                .hasAnyRole("MODERATOR", "SENIOR_MODERATOR")
                .requestMatchers("/api/v1/reports", "/api/v1/reports/**")
                .hasRole("STUDENT")
                .anyRequest()
                .denyAll())
        .oauth2ResourceServer(o ->
            o.jwt(j -> {})
                .authenticationEntryPoint(
                    (r, s, e) -> Problems.write(s, json, 401, "UNAUTHENTICATED")))
        .exceptionHandling(e ->
            e.authenticationEntryPoint((r, s, x) -> Problems.write(s, json, 401, "UNAUTHENTICATED"))
                .accessDeniedHandler((r, s, x) -> Problems.write(s, json, 403, "FORBIDDEN")))
        .addFilterBefore(new RequestGuards.Correlation(), BearerTokenAuthenticationFilter.class)
        .addFilterAfter(
            new RequestGuards.RateLimit(json, store, properties),
            BearerTokenAuthenticationFilter.class)
        .addFilterAfter(
            new RequestGuards.BodyLimit(json, properties.maxBodyBytes()), AuthorizationFilter.class)
        .build();
  }
}
