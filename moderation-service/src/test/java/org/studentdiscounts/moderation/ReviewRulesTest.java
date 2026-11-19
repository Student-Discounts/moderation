package org.studentdiscounts.moderation;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ReviewRulesTest {
  final UUID author = UUID.randomUUID(), reviewer = UUID.randomUUID(), version = UUID.randomUUID();
  final ReviewRules.Actor moderator = new ReviewRules.Actor(reviewer, Set.of("MODERATOR"));

  ReviewRules.State state(String status, boolean pending) {
    return new ReviewRules.State(author, reviewer, version, 2, 4, 9, status, pending, false);
  }

  @Test
  void rejectsSelfReviewEvenSenior() {
    assertThrows(
        RuntimeException.class,
        () ->
            ReviewRules.check(
                state("PENDING_REVIEW", false),
                new ReviewRules.Actor(author, Set.of("SENIOR_MODERATOR")),
                ReviewRules.Action.CLAIM,
                version,
                2,
                4,
                null));
  }

  @Test
  void claimMustBeConfirmedBeforeDecision() {
    assertThrows(
        RuntimeException.class,
        () ->
            ReviewRules.check(
                state("PENDING_REVIEW", true),
                moderator,
                ReviewRules.Action.APPROVE,
                version,
                2,
                4,
                null));
  }

  @Test
  void onlyAssignedReviewerCanDecide() {
    assertThrows(
        RuntimeException.class,
        () ->
            ReviewRules.check(
                state("IN_REVIEW", false),
                new ReviewRules.Actor(UUID.randomUUID(), Set.of("SENIOR_MODERATOR")),
                ReviewRules.Action.APPROVE,
                version,
                2,
                4,
                null));
  }

  @Test
  void rejectsStaleVersionAndLocalConcurrencyToken() {
    assertThrows(
        RuntimeException.class,
        () ->
            ReviewRules.check(
                state("IN_REVIEW", false),
                moderator,
                ReviewRules.Action.APPROVE,
                UUID.randomUUID(),
                2,
                4,
                null));
    assertThrows(
        RuntimeException.class,
        () ->
            ReviewRules.check(
                state("IN_REVIEW", false),
                moderator,
                ReviewRules.Action.APPROVE,
                version,
                2,
                3,
                null));
  }

  @Test
  void reasonsAreMandatoryExceptApprovalAndClaim() {
    for (ReviewRules.Action action :
        Set.of(
            ReviewRules.Action.REJECT,
            ReviewRules.Action.CHANGES_REQUESTED,
            ReviewRules.Action.REQUEST_EVIDENCE,
            ReviewRules.Action.ESCALATE)) {
      assertThrows(
          RuntimeException.class,
          () ->
              ReviewRules.check(state("IN_REVIEW", false), moderator, action, version, 2, 4, "  "));
    }
  }

  @Test
  void confirmedAssignedApprovalIsAllowed() {
    assertDoesNotThrow(() ->
        ReviewRules.check(
            state("IN_REVIEW", false), moderator, ReviewRules.Action.APPROVE, version, 2, 4, null));
    assertEquals("SubmissionApproved", ReviewRules.eventType(ReviewRules.Action.APPROVE));
    assertEquals("CHANGES_REQUESTED", ReviewRules.target(ReviewRules.Action.REQUEST_EVIDENCE));
  }

  @Test
  void correctedInReviewSnapshotCanBeClaimedAgainWithoutStatusTransition() {
    var corrected =
        new ReviewRules.State(author, null, version, 2, 4, 9, "IN_REVIEW", false, false);
    assertDoesNotThrow(() ->
        ReviewRules.check(corrected, moderator, ReviewRules.Action.CLAIM, version, 2, 4, null));
    var escalated = new ReviewRules.State(author, null, version, 2, 4, 9, "IN_REVIEW", false, true);
    assertThrows(
        RuntimeException.class,
        () ->
            ReviewRules.check(escalated, moderator, ReviewRules.Action.CLAIM, version, 2, 4, null));
  }
}
