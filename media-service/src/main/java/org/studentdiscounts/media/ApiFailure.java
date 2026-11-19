package org.studentdiscounts.media;

import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.support.MissingServletRequestPartException;
import software.amazon.awssdk.core.exception.SdkException;

public final class ApiFailure extends RuntimeException {
  final HttpStatus status;
  final String code;

  public ApiFailure(HttpStatus status, String code) {
    super(code);
    this.status = status;
    this.code = code;
  }

  public static ApiFailure notFound() {
    return new ApiFailure(HttpStatus.NOT_FOUND, "MEDIA_NOT_FOUND");
  }

  public static ApiFailure unavailable() {
    return new ApiFailure(HttpStatus.SERVICE_UNAVAILABLE, "DEPENDENCY_UNAVAILABLE");
  }

  static ProblemDetail problem(HttpStatus status, String code) {
    var detail = ProblemDetail.forStatus(status);
    detail.setTitle(status.getReasonPhrase());
    detail.setDetail(code);
    detail.setProperty("code", code);
    return detail;
  }

  @RestControllerAdvice
  public static class Advice {
    @ExceptionHandler(ApiFailure.class)
    public org.springframework.http.ResponseEntity<ProblemDetail> failure(ApiFailure e) {
      var result = org.springframework.http.ResponseEntity.status(e.status);
      if (e.status == HttpStatus.TOO_MANY_REQUESTS) result.header("Retry-After", "60");
      return result.body(problem(e.status, e.code));
    }

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ProblemDetail tooLarge() {
      return problem(HttpStatus.PAYLOAD_TOO_LARGE, "UPLOAD_TOO_LARGE");
    }

    @ExceptionHandler({
      MissingServletRequestPartException.class,
      MethodArgumentTypeMismatchException.class
    })
    public ProblemDetail badRequest() {
      return problem(HttpStatus.BAD_REQUEST, "INVALID_REQUEST");
    }

    @ExceptionHandler({DataAccessException.class, SdkException.class})
    public ProblemDetail dependency() {
      return problem(HttpStatus.SERVICE_UNAVAILABLE, "DEPENDENCY_UNAVAILABLE");
    }
  }
}
