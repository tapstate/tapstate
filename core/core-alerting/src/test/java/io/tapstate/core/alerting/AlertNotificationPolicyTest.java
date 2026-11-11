package io.tapstate.core.alerting;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.alerting.DeliveryEvidence.*;

class AlertNotificationPolicyTest {
    static final Instant T = Instant.parse("2026-10-21T00:00:00Z");
    @Test void retryDefaultsCountTheFirstAttemptAndRespectPositiveJitterAndTargetLimits() {
        ChannelRetryPolicy email = ChannelRetryPolicy.emailDefaults();
        ChannelRetryPolicy webhook = ChannelRetryPolicy.webhookDefaults();
        int[] emailSeconds = {1800, 3600, 7200, 14400};
        int[] webhookSeconds = {30, 120, 600, 1800};
        for (int i = 1; i <= 4; i++) {
            assertThat(email.nextAttemptAt(i, T, 0, T.plusSeconds(86400), null, null)).contains(T.plusSeconds(emailSeconds[i - 1]));
            assertThat(webhook.nextAttemptAt(i, T, 2000, T.plusSeconds(86400), null, null)).contains(T.plusMillis(webhookSeconds[i - 1] * 1200L));
        }
        assertThat(email.maximumTotalAttempts()).isEqualTo(15);
        assertThat(email.nextAttemptAt(5, T, 0, T.plusSeconds(86400), null, null)).isEmpty();
        assertThat(webhook.nextAttemptAt(1, T, 0, T.plusSeconds(86400), T.plusSeconds(60), T.plusSeconds(90))).contains(T.plusSeconds(90));
        assertThat(webhook.nextAttemptAt(1, T, 0, T.plusSeconds(86400), T.plusSeconds(172800), null)).isEmpty();
        assertThat(webhook.nextAttemptAt(1, T, 0, T.plusSeconds(30), null, null)).isEmpty();
    }
    @Test void singleAttemptNeverGetsAnAutomaticOrManualCycleAndCutoffsAreAbsolute() {
        ChannelRetryPolicy single = ChannelRetryPolicy.singleAttempt(Duration.ofMinutes(5), Duration.ofSeconds(30));
        assertThat(single.maximumTotalAttempts()).isEqualTo(1);
        assertThat(single.maximumManualCycles()).isZero();
        assertThat(single.nextAttemptAt(1, T, 0, T.plusSeconds(300), null, null)).isEmpty();
        assertThat(single.attemptDeadline(T, T.plusSeconds(20))).contains(T.plusSeconds(20));
        assertThat(single.attemptDeadline(T.plusSeconds(20), T.plusSeconds(20))).isEmpty();
    }
    @Test void acceptedAndUnknownEvidenceSurviveLaterRejectedAttempts() {
        assertThat(aggregate(List.of(Outcome.UNKNOWN, Outcome.NOT_ACCEPTED))).isEqualTo(Outcome.UNKNOWN);
        assertThat(aggregate(List.of(Outcome.ACCEPTED, Outcome.UNKNOWN, Outcome.NOT_ACCEPTED))).isEqualTo(Outcome.ACCEPTED);
        assertThat(aggregate(List.of(Outcome.NOT_ATTEMPTED))).isEqualTo(Outcome.NOT_ATTEMPTED);
    }
    @Test void recoveryAggregatesEveryRoundAndWaitsOnlyForOriginalGrantedDeadlines() {
        List<Previous> rounds = List.of(new Previous(Outcome.NOT_ACCEPTED, true, null), new Previous(Outcome.ACCEPTED, false, null),
                new Previous(Outcome.NOT_ATTEMPTED, false, T.plusSeconds(30)));
        assertThat(recovery(rounds, T.plusSeconds(20), false, false)).isEqualTo(new Recovery(RecoveryKind.WAIT, T.plusSeconds(30)));
        assertThat(recovery(rounds, T.plusSeconds(30), false, false).kind()).isEqualTo(RecoveryKind.RECOVERY);
        assertThat(recovery(rounds, T.plusSeconds(30), true, false).kind()).isEqualTo(RecoveryKind.NONE);
        assertThat(recovery(List.of(new Previous(Outcome.UNKNOWN, false, null), new Previous(Outcome.NOT_ACCEPTED, false, null)), T, false, false).kind()).isEqualTo(RecoveryKind.UNKNOWN_SUMMARY);
        assertThat(recovery(List.of(new Previous(Outcome.NOT_ATTEMPTED, true, null)), T, false, false).kind()).isEqualTo(RecoveryKind.FAILURE_SUMMARY);
        assertThat(recovery(List.of(new Previous(Outcome.NOT_ATTEMPTED, false, null)), T, false, false).kind()).isEqualTo(RecoveryKind.NONE);
        assertThat(recovery(List.of(new Previous(Outcome.NOT_ATTEMPTED, false, T.plusSeconds(10))), T.plusSeconds(10), false, false).kind()).isEqualTo(RecoveryKind.UNKNOWN_SUMMARY);
    }
    @Test void groupClocksDoNotSlideWithMembersOrRetriesAndSimultaneousReasonsPlanOnce() {
        var p = new AlertGroupingClock.Policy(Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(120));
        AlertGroupingClock s = AlertGroupingClock.empty().changed(T).changed(T.plusSeconds(3)).changed(T.plusSeconds(6)).changed(T.plusSeconds(9));
        assertThat(s.due(p, T.plusSeconds(9), true, false)).isEmpty();
        s = s.planned(s.due(p, T.plusSeconds(10), true, false).orElseThrow(), T.plusSeconds(10));
        s = s.changed(T.plusSeconds(12));
        assertThat(s.due(p, T.plusSeconds(39), true, false)).isEmpty();
        s = s.planned(s.due(p, T.plusSeconds(40), true, false).orElseThrow(), T.plusSeconds(40));
        assertThat(s.due(p, T.plusSeconds(159), true, false)).isEmpty();
        assertThat(s.due(p, T.plusSeconds(160), true, true)).isEmpty();
        s = s.coalescedPredecessorEnded(T.plusSeconds(170), p);
        assertThat(s.due(p, T.plusSeconds(289), true, false)).isEmpty();
        assertThat(s.due(p, T.plusSeconds(290), true, false).orElseThrow().round()).isEqualTo(3);
        s = s.changed(T.plusSeconds(200));
        assertThat(s.due(p, T.plusSeconds(290), true, false).orElseThrow().reason()).isEqualTo(AlertGroupingClock.Reason.CHANGE);
        assertThat(s.due(p, T.plusSeconds(290), false, false)).isEmpty();
    }
    @Test void deliveryTargetKeysNeverMergeIndependentTargetsOrRoutes() {
        var round = new DeliveryKey.NotificationRound(new AlarmKey.Scope("org", "cluster"), "episode-1", "inc-1", "route", "route-inc", DeliveryKey.Kind.FAULT, 1);
        var a = new DeliveryKey(round, "email", "receiver-inc", "a");
        assertThat(a).isNotEqualTo(new DeliveryKey(round, "email", "receiver-inc", "b"));
        assertThat(a).isNotEqualTo(new DeliveryKey(round, "webhook", "receiver-inc", "a"));
        assertThat(a).isNotEqualTo(new DeliveryKey(round, "email", "new-receiver-inc", "a"));
        assertThat(a).isEqualTo(new DeliveryKey(round, "email", "receiver-inc", "a"));
    }
    @Test void anExpiredOutstandingPermitIsUnknownEvenAfterAnEarlierDefinitiveRejection() {
        var previous = List.of(new Previous(Outcome.NOT_ACCEPTED, false, T.plusSeconds(30)));
        assertThat(recovery(previous, T.plusSeconds(20), false, false).kind()).isEqualTo(RecoveryKind.WAIT);
        assertThat(recovery(previous, T.plusSeconds(30), false, false).kind()).isEqualTo(RecoveryKind.UNKNOWN_SUMMARY);
    }
}
