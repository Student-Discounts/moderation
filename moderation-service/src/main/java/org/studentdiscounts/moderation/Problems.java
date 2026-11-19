package org.studentdiscounts.moderation;

import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.ConstraintViolationException;
import java.io.IOException;
import org.springframework.http.*;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import tools.jackson.databind.ObjectMapper;

@RestControllerAdvice
class Problems {
  static ProblemDetail detail(int status, String code) {
    var problem = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(status), code);
    problem.setProperty("code", code);
    problem.setProperty("correlationId", org.slf4j.MDC.get("correlationId"));
    return problem;
  }

  static void write(HttpServletResponse response, ObjectMapper json, int status, String code)
      throws IOException {
    response.setStatus(status);
    response.setContentType("application/problem+json");
    json.writeValue(response.getOutputStream(), detail(status, code));
  }

  @ExceptionHandler(ApiException.class)
  ResponseEntity<ProblemDetail> api(ApiException e) {
    return ResponseEntity.status(e.status).body(detail(e.status, e.code));
  }

  @ExceptionHandler({MethodArgumentNotValidException.class, ConstraintViolationException.class})
  ResponseEntity<ProblemDetail> validation(Exception e) {
    return ResponseEntity.unprocessableContent().body(detail(422, "VALIDATION_FAILED"));
  }

  @ExceptionHandler({
    HttpMessageNotReadableException.class,
    MethodArgumentTypeMismatchException.class,
    org.springframework.web.bind.MissingRequestHeaderException.class
  })
  ResponseEntity<ProblemDetail> malformed(Exception e) {
    return ResponseEntity.badRequest().body(detail(400, "MALFORMED_REQUEST"));
  }

  @ExceptionHandler(AccessDeniedException.class)
  ResponseEntity<ProblemDetail> forbidden() {
    return ResponseEntity.status(403).body(detail(403, "FORBIDDEN"));
  }

  @ExceptionHandler(org.springframework.dao.DataAccessException.class)
  ResponseEntity<ProblemDetail> database() {
    return ResponseEntity.status(503).body(detail(503, "DATABASE_UNAVAILABLE"));
  }
}
