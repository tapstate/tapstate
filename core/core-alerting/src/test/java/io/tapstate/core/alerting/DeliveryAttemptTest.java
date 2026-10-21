package io.tapstate.core.alerting;

import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.alerting.DeliveryEvidence.*;
import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeliveryAttemptTest {
    static final Instant T = Instant.parse("2026-10-21T00:00:00Z");
    static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("org", "cluster");
    static DeliveryAttempt attempt(String id, int ordinal) {
        return DeliveryAttempt.grant(SCOPE, "delivery", id, ordinal, 1, "owner-a", ChannelRetryPolicy.webhookDefaults(),
                T, T.plusSeconds(86400), T).orElseThrow();
    }
    @Test void candidatePermitFixesTheOriginalDeadlineWithoutClaimingItWasPersistedOrCalled() {
        var attempt = attempt("a1", 1);
        assertThat(attempt.pending()).isTrue(); assertThat(attempt.outcome()).isEqualTo(Outcome.UNKNOWN);
        assertThat(attempt.permit().deadline()).isEqualTo(T.plusSeconds(30));
        assertThat(attempt.settleDeadline(T.plusSeconds(29))).isSameAs(attempt);
        assertThat(recovery(List.of(attempt.previousEvidence()), T.plusSeconds(29), false, false).kind()).isEqualTo(RecoveryKind.WAIT);
        var expired = attempt.settleDeadline(T.plusSeconds(100));
        assertThat(expired.endedAt()).isEqualTo(T.plusSeconds(30));
        assertThat(expired.settleDeadline(T.plusSeconds(200))).isSameAs(expired);
        assertThat(expired.permit()).isEqualTo(attempt.permit());
        assertThat(ChannelRetryPolicy.webhookDefaults().nextAttemptAt(1, expired.endedAt(), 0, T.plusSeconds(86400), null, null))
                .contains(T.plusSeconds(60));
    }
    @Test void finalEvidenceSurvivesRejectionDeadlineReplaysAndTheFixedEndDoesNotSlide() {
        var original = attempt("a1", 1);
        var known = original.receipt(original.permit(), Outcome.ACCEPTED, T.plusSeconds(1)).attempt();
        assertThat(known.endedAt()).isEqualTo(T.plusSeconds(1));
        assertThat(known.settleDeadline(T.plusSeconds(100))).isSameAs(known);
        assertThat(known.receipt(original.permit(), Outcome.NOT_ACCEPTED, T.plusSeconds(100)).attempt()).isSameAs(known);
        var unknown = original.settleDeadline(T.plusSeconds(100));
        var late = unknown.receipt(original.permit(), Outcome.ACCEPTED, T.plusSeconds(101)).attempt();
        assertThat(late.outcome()).isEqualTo(Outcome.ACCEPTED);
        assertThat(late.endedAt()).isEqualTo(T.plusSeconds(30));
        assertThat(late.permit()).isSameAs(unknown.permit());
        assertThat(unknown.receipt(original.permit(), Outcome.NOT_ACCEPTED, T.plusSeconds(101)).attempt()).isSameAs(unknown);
    }
    @Test void receiptAfterTheCutoffDoesNotDependOnWhetherTheTimeoutCallbackRanFirst() {
        var original = attempt("a1", 1);
        Instant lateAt = T.plusSeconds(31);
        var timeoutFirst = original.settleDeadline(lateAt).receipt(original.permit(), Outcome.NOT_ACCEPTED, lateAt).attempt();
        var receiptFirst = original.receipt(original.permit(), Outcome.NOT_ACCEPTED, lateAt).attempt();
        assertThat(receiptFirst.outcome()).isEqualTo(timeoutFirst.outcome()).isEqualTo(Outcome.UNKNOWN);
        assertThat(receiptFirst.endedAt()).isEqualTo(timeoutFirst.endedAt()).isEqualTo(original.permit().deadline());
        assertThat(original.receipt(original.permit(), Outcome.ACCEPTED, lateAt).attempt().outcome()).isEqualTo(Outcome.ACCEPTED);
    }
    @Test void foreignScopeDeliveryAttemptAndOwnerGenerationCannotWriteAnotherPermit() {
        var current = attempt("a2", 2); var original = current.permit();
        for (var foreign : List.of(
                new DeliveryAttempt.Permit(new AlarmKey.Scope("other", "cluster"), original.deliveryId(), original.attemptId(), 2, 1, "owner-a", T, original.deadline()),
                new DeliveryAttempt.Permit(SCOPE, "new-delivery", original.attemptId(), 2, 1, "owner-a", T, original.deadline()),
                new DeliveryAttempt.Permit(SCOPE, original.deliveryId(), "a1", 1, 1, "owner-a", T, original.deadline()),
                new DeliveryAttempt.Permit(SCOPE, original.deliveryId(), original.attemptId(), 2, 2, "owner-b", T, original.deadline()))) {
            var update = current.receipt(foreign, Outcome.ACCEPTED, T.plusSeconds(1));
            assertThat(update.ignored()).isTrue(); assertThat(update.attempt()).isSameAs(current);
        }
        var previous = attempt("a1", 1).settleDeadline(T.plusSeconds(31));
        var late = previous.receipt(previous.permit(), Outcome.ACCEPTED, T.plusSeconds(40)).attempt();
        assertThat(late.outcome()).isEqualTo(Outcome.ACCEPTED);
        assertThat(current.pending()).isTrue(); assertThat(current.outcome()).isEqualTo(Outcome.UNKNOWN);
    }
    @Test void deadlineCanOnlyShrinkToOriginalIntentAndSingleAttemptCannotAppendOrdinals() {
        var single = ChannelRetryPolicy.singleAttempt(Duration.ofMinutes(5), Duration.ofSeconds(30));
        var bounded = DeliveryAttempt.grant(SCOPE, "test", "a1", 1, 1, "owner-a", single, T, T.plusSeconds(20), T).orElseThrow();
        assertThat(bounded.permit().deadline()).isEqualTo(T.plusSeconds(20));
        assertThat(DeliveryAttempt.grant(SCOPE, "test", "a1", 1, 1, "owner-a", single, T, T.plusSeconds(20), T.plusSeconds(20))).isEmpty();
        assertThatThrownBy(() -> DeliveryAttempt.grant(SCOPE, "test", "a2", 2, 1, "owner-a", single, T, T.plusSeconds(20), T))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> DeliveryAttempt.grant(SCOPE, "test", "a1", 1, 1, "owner-a", single, T, T.plusSeconds(301), T))
                .isInstanceOf(TapstateException.class);
    }
    @Test void aPermittedDefinitiveFailureIsNotWashedIntoUnknownBeforeTheDeadline() {
        var original = attempt("a1", 1);
        var failure = original.receipt(original.permit(), Outcome.NOT_ACCEPTED, T.plusSeconds(1)).attempt();
        assertThat(failure.outcome()).isEqualTo(Outcome.NOT_ACCEPTED);
        assertThat(failure.endedAt()).isEqualTo(T.plusSeconds(1));
        assertThat(recovery(List.of(failure.previousEvidence()), T.plusSeconds(2), false, false).kind()).isEqualTo(RecoveryKind.FAILURE_SUMMARY);
        assertThatThrownBy(() -> original.receipt(original.permit(), Outcome.NOT_ATTEMPTED, T)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> original.receipt(original.permit(), Outcome.ACCEPTED, T.minusSeconds(1))).isInstanceOf(TapstateException.class);
    }
}
