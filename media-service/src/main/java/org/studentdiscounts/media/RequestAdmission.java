package org.studentdiscounts.media;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@Order(-90)
public class RequestAdmission extends OncePerRequestFilter {
  private final JdbcTemplate jdbc;
  private final MediaProperties properties;
  private final Semaphore uploads;
  private final Semaphore previews;

  public RequestAdmission(JdbcTemplate jdbc, MediaProperties properties) {
    this.jdbc = jdbc;
    this.properties = properties;
    uploads = new Semaphore(properties.limits().uploadConcurrency());
    previews = new Semaphore(properties.limits().previewConcurrency());
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    String correlation = request.getHeader("X-Correlation-ID");
    if (correlation == null || !correlation.matches("[A-Za-z0-9._:-]{1,64}"))
      correlation = MediaService.newId().toString();
    MDC.put("correlationId", correlation);
    response.setHeader("X-Correlation-ID", correlation);
    response.setHeader("Cache-Control", "private, no-store");
    boolean acquired = false, previewAcquired = false;
    try {
      if (request.getRequestURI().startsWith("/api/v1/media-assets")) {
        var authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication instanceof JwtAuthenticationToken jwt) {
          UUID actor = UUID.fromString(jwt.getToken().getSubject());
          if (!allow(actor, "minute", properties.limits().requestsPerMinute())) {
            response.setHeader("Retry-After", "60");
            MediaSecurity.securityProblem(response, 429, "RATE_LIMITED");
            return;
          }
          if ("GET".equals(request.getMethod()) && request.getRequestURI().endsWith("/preview")) {
            if (!previews.tryAcquire()) {
              MediaSecurity.securityProblem(response, 503, "PREVIEW_CAPACITY_EXCEEDED");
              return;
            }
            previewAcquired = true;
          }
          if ("POST".equals(request.getMethod())) {
            if (request.getContentLengthLong() > properties.limits().maxBytes() + 65536L) {
              MediaSecurity.securityProblem(response, 413, "UPLOAD_TOO_LARGE");
              return;
            }
            if (!uploads.tryAcquire()) {
              MediaSecurity.securityProblem(response, 503, "UPLOAD_CAPACITY_EXCEEDED");
              return;
            }
            acquired = true;
            if (!allow(actor, "day", properties.limits().uploadsPerDay())) {
              response.setHeader("Retry-After", "3600");
              MediaSecurity.securityProblem(response, 429, "UPLOAD_QUOTA_EXCEEDED");
              return;
            }
          }
        }
      }
      chain.doFilter(request, response);
    } catch (org.springframework.dao.DataAccessException e) {
      MediaSecurity.securityProblem(response, 503, "DEPENDENCY_UNAVAILABLE");
    } finally {
      if (acquired) uploads.release();
      if (previewAcquired) previews.release();
      MDC.remove("correlationId");
    }
  }

  private boolean allow(UUID actor, String kind, int limit) {
    return Boolean.TRUE.equals(
        jdbc.queryForObject(
            """
            insert into media_rate_window(actor_id,window_start,kind,count)
            values (?,date_trunc(?,now() at time zone 'UTC') at time zone 'UTC',?,1)
            on conflict(actor_id,window_start,kind) do update set count=media_rate_window.count+1
            returning count <= ?
            """,
            Boolean.class,
            actor,
            kind,
            kind,
            limit));
  }

  @Scheduled(fixedDelay = 3600000)
  void cleanup() {
    jdbc.update(
        "delete from media_rate_window where ctid in (select ctid from media_rate_window where"
            + " window_start < now()-interval '2 days' limit 5000)");
  }
}
