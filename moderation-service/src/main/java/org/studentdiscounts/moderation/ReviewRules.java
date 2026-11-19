package org.studentdiscounts.moderation;

import java.util.Set;
import java.util.UUID;

final class ReviewRules {
  record Actor(UUID id, Set<String> roles) {
    boolean senior() {
      return roles.contains("SENIOR_MODERATOR");
    }

    boolean reviewer() {
      return senior() || roles.contains("MODERATOR");
    }
  }

  record State(
      UUID author,
      UUID reviewer,
      UUID versionId,
      int versionNo,
      long localVersion,
      long submissionVersion,
      String status,
      boolean pending,
      boolean escalated) {}

  enum Action {
    CLAIM,
    APPROVE,
    REJECT,
    CHANGES_REQUESTED,
    REQUEST_EVIDENCE,
    ESCALATE,
    REOPEN
  }

  static void check(
      State state,
      Actor actor,
      Action action,
      UUID versionId,
      int versionNo,
      long ifMatch,
      String reason) {
    ApiException.require(actor.reviewer(), 403, "REVIEWER_REQUIRED");
    ApiException.require(!actor.id().equals(state.author()), 403, "SELF_REVIEW_FORBIDDEN");
    ApiException.require(state.localVersion() == ifMatch, 412, "STALE_ROW_VERSION");
    ApiException.require(
        versionId.equals(state.versionId()) && versionNo == state.versionNo(),
        409,
        "STALE_REVIEWED_VERSION");
    ApiException.require(!state.pending(), 409, "COMMAND_PENDING");
    if (action != Action.APPROVE && action != Action.CLAIM) {
      ApiException.require(
          reason != null && !reason.isBlank() && reason.length() <= 2000, 422, "REASON_REQUIRED");
    }
    switch (action) {
      case CLAIM -> {
        ApiException.require(
            (state.status().equals("PENDING_REVIEW") || state.status().equals("IN_REVIEW"))
                && state.reviewer() == null,
            409,
            "NOT_CLAIMABLE");
        ApiException.require(!state.escalated() || actor.senior(), 403, "SENIOR_REQUIRED");
      }
      case REOPEN -> {
        ApiException.require(actor.senior(), 403, "SENIOR_REQUIRED");
        ApiException.require(state.status().equals("SUSPENDED"), 409, "INVALID_TRANSITION");
      }
      default -> {
        ApiException.require(state.status().equals("IN_REVIEW"), 409, "REVIEW_NOT_CONFIRMED");
        ApiException.require(
            actor.id().equals(state.reviewer()), 403, "ASSIGNED_REVIEWER_REQUIRED");
        ApiException.require(!state.escalated() || actor.senior(), 403, "SENIOR_REQUIRED");
      }
    }
  }

  static String eventType(Action action) {
    return switch (action) {
      case CLAIM -> "ReviewStarted";
      case APPROVE -> "SubmissionApproved";
      case REJECT -> "SubmissionRejected";
      case CHANGES_REQUESTED, REQUEST_EVIDENCE -> "SubmissionChangesRequested";
      case REOPEN -> "SubmissionReviewReopened";
      default -> throw new ApiException(409, "LOCAL_ACTION");
    };
  }

  static String target(Action action) {
    return switch (action) {
      case CLAIM -> "IN_REVIEW";
      case APPROVE -> "APPROVED";
      case REJECT -> "REJECTED";
      case CHANGES_REQUESTED, REQUEST_EVIDENCE -> "CHANGES_REQUESTED";
      case REOPEN -> "PENDING_REVIEW";
      default -> throw new ApiException(409, "LOCAL_ACTION");
    };
  }
}
