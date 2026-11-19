package org.studentdiscounts.moderation;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.filter.OncePerRequestFilter;
import tools.jackson.databind.ObjectMapper;

final class RequestGuards {
  static final class Correlation extends OncePerRequestFilter {
    @Override
    protected void doFilterInternal(
        HttpServletRequest request, HttpServletResponse response, FilterChain chain)
        throws ServletException, IOException {
      String incoming = request.getHeader("X-Correlation-ID");
      String correlation =
          incoming != null && incoming.matches("[A-Za-z0-9._:-]{1,64}")
              ? incoming
              : UUID.randomUUID().toString();
      response.setHeader("X-Correlation-ID", correlation);
      MDC.put("correlationId", correlation);
      try {
        chain.doFilter(request, response);
      } finally {
        MDC.remove("correlationId");
      }
    }
  }

  static final class BodyLimit extends OncePerRequestFilter {
    private final ObjectMapper json;
    private final int maxBytes;

    BodyLimit(ObjectMapper json, int maxBytes) {
      this.json = json;
      this.maxBytes = maxBytes;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
      return !"POST".equals(request.getMethod());
    }

    @Override
    protected void doFilterInternal(
        HttpServletRequest request, HttpServletResponse response, FilterChain chain)
        throws ServletException, IOException {
      long length = request.getContentLengthLong();
      if (length > maxBytes) {
        Problems.write(response, json, 413, "BODY_TOO_LARGE");
        return;
      }
      if (length >= 0) {
        chain.doFilter(request, response);
        return;
      }
      // For chunked/unknown lengths, check the entire bounded body before MVC can execute a
      // command.
      byte[] body = request.getInputStream().readNBytes(maxBytes + 1);
      if (body.length > maxBytes) {
        Problems.write(response, json, 413, "BODY_TOO_LARGE");
        return;
      }
      chain.doFilter(new BufferedRequest(request, body), response);
    }
  }

  private static final class BufferedRequest extends HttpServletRequestWrapper {
    private final ServletInputStream stream;

    BufferedRequest(HttpServletRequest request, byte[] body) {
      super(request);
      var input = new ByteArrayInputStream(body);
      stream =
          new ServletInputStream() {
            @Override
            public int read() {
              return input.read();
            }

            @Override
            public int read(byte[] bytes, int offset, int length) {
              return input.read(bytes, offset, length);
            }

            @Override
            public boolean isFinished() {
              return input.available() == 0;
            }

            @Override
            public boolean isReady() {
              return true;
            }

            @Override
            public void setReadListener(ReadListener listener) {
              throw new UnsupportedOperationException("Synchronous request body");
            }
          };
    }

    @Override
    public ServletInputStream getInputStream() {
      return stream;
    }

    @Override
    public BufferedReader getReader() {
      return new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
    }
  }

  static final class RateLimit extends OncePerRequestFilter {
    private final ObjectMapper json;
    private final ReviewStore store;
    private final ModerationProperties properties;

    RateLimit(ObjectMapper json, ReviewStore store, ModerationProperties properties) {
      this.json = json;
      this.store = store;
      this.properties = properties;
    }

    @Override
    protected void doFilterInternal(
        HttpServletRequest request, HttpServletResponse response, FilterChain chain)
        throws ServletException, IOException {
      var authentication = SecurityContextHolder.getContext().getAuthentication();
      if (authentication instanceof JwtAuthenticationToken jwt
          && request.getRequestURI().startsWith("/api/")) {
        Integer count;
        try {
          count =
              store
                  .jdbc()
                  .queryForObject(
                      "insert into request_windows(actor_id,minute,count)"
                          + " values(?,date_trunc('minute',now()),1) on conflict(actor_id,minute)"
                          + " do update set count=request_windows.count+1 returning count",
                      Integer.class,
                      UUID.fromString(jwt.getToken().getSubject()));
        } catch (org.springframework.dao.DataAccessException unavailable) {
          Problems.write(response, json, 503, "RATE_LIMIT_UNAVAILABLE");
          return;
        }
        if (count > properties.requestsPerMinute()) {
          response.setHeader("Retry-After", "60");
          Problems.write(response, json, 429, "RATE_LIMIT_EXCEEDED");
          return;
        }
      }
      chain.doFilter(request, response);
    }
  }
}
