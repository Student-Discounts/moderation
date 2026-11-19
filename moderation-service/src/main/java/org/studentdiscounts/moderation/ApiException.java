package org.studentdiscounts.moderation;

final class ApiException extends RuntimeException {
  final int status;
  final String code;

  ApiException(int status, String code) {
    super(code);
    this.status = status;
    this.code = code;
  }

  static void require(boolean condition, int status, String code) {
    if (!condition) throw new ApiException(status, code);
  }
}
