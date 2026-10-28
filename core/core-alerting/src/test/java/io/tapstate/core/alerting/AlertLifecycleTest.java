package io.tapstate.core.alerting;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.alerting.AlertSignal.Condition.*;
import static io.tapstate.core.alerting.AlertSnapshot.*;

class AlertLifecycleTest {
    @Test void missingProducerSequencesBreakBothTriggerAndRecoveryContinuity() {
        var pending = sample(AlertSnapshot.initial(rule(1, 10, 10)), 1, 0, BAD);
        pending = sample(pending, 3, 10, BAD);
        assertThat(pending.status()).isEqualTo(Status.PENDING);
        assertThat(pending.pendingSince()).isEqualTo(T.plusSeconds(10));
        var firing = sample(AlertSnapshot.initial(rule(1, 0, 10)), 1, 0, BAD);
        firing = sample(firing, 2, 1, GOOD);
        firing = sample(firing, 4, 11, GOOD);
        assertThat(firing.status()).isEqualTo(Status.FIRING);
        assertThat(firing.recoverySince()).isEqualTo(T.plusSeconds(11));
    }
    @Test void refusedCurrentInputBreaksContinuityWithoutAdvancingTheAcceptedFrontier() {
        var pending = sample(AlertSnapshot.initial(rule(1, 10, 10)), 1, 0, BAD);
        var lost = AlertLifecycle.discontinuity(pending, T.plusSeconds(5)).snapshot();
        assertThat(lost.lastSequence()).isOne();
        assertThat(lost.lastObservedAt()).isEqualTo(T);
        assertThat(lost.pendingSince()).isNull();
        assertThat(lost.quality()).isEqualTo(Quality.UNKNOWN);
    }
    static final Instant T = Instant.parse("2026-10-21T00:00:00Z");
    static AlarmKey key(String authority, String incarnation, String qualifier) {
        return new AlarmKey(new AlarmKey.Scope(authority, "cluster"),
                new AlarmKey.Resource("pipeline", "orders", incarnation), "pipeline.failed", qualifier);
    }
    static AlertRule rule(long generation, int trigger, int recovery) {
        return new AlertRule(key("org-a", "resource-1", "rule-a"), generation, generation, "producer-" + generation,
                Duration.ofSeconds(trigger), Duration.ofSeconds(recovery), Duration.ofSeconds(15), Duration.ofSeconds(10));
    }
    static AlertSignal signal(AlertRule rule, long sequence, int time, AlertSignal.Condition condition, AlarmSeverity severity) {
        return new AlertSignal(rule.key(), rule.revision(), rule.generation(), rule.producerIncarnation(),
                sequence, T.plusSeconds(time), condition, severity);
    }
    static AlertSnapshot sample(AlertSnapshot old, long sequence, int time, AlertSignal.Condition condition) {
        return AlertLifecycle.evaluate(old, signal(old.rule(), sequence, time, condition, AlarmSeverity.MAJOR), T.plusSeconds(time)).snapshot();
    }
    @Test void newContinuousSamplesAreRequiredAndTicksNeverRaiseOrRecover() {
        AlertSnapshot s = sample(AlertSnapshot.initial(rule(1, 10, 10)), 1, 0, BAD);
        assertThat(s.status()).isEqualTo(Status.PENDING);
        AlertSnapshot duplicate = AlertLifecycle.evaluate(s, signal(s.rule(), 1, 0, BAD, AlarmSeverity.MAJOR), T.plusSeconds(10)).snapshot();
        assertThat(duplicate).isEqualTo(s);
        assertThat(AlertLifecycle.tick(s, T.plusSeconds(10)).snapshot().status()).isEqualTo(Status.PENDING);
        s = sample(s, 2, 5, BAD);
        s = sample(s, 3, 10, BAD);
        assertThat(s.status()).isEqualTo(Status.FIRING);
        assertThat(s.episode().number()).isEqualTo(1);
        s = AlertLifecycle.tick(s, T.plusSeconds(26)).snapshot();
        assertThat(s.quality()).isEqualTo(Quality.STALE);
        assertThat(s.status()).isEqualTo(Status.FIRING);
        assertThat(s.episode().recoveredAt()).isNull();
        s = sample(s, 4, 27, GOOD);
        assertThat(s.status()).isEqualTo(Status.FIRING);
        s = sample(s, 5, 32, GOOD);
        s = sample(s, 6, 37, GOOD);
        assertThat(s.status()).isEqualTo(Status.RESOLVED);
        assertThat(s.episode().recoveredAt()).isEqualTo(T.plusSeconds(37));
    }
    @Test void unknownAndGapsRestartContinuityInsteadOfFakingRecovery() {
        AlertSnapshot s = sample(AlertSnapshot.initial(rule(1, 10, 10)), 1, 0, BAD);
        s = sample(s, 2, 5, UNKNOWN);
        s = sample(s, 3, 10, BAD);
        assertThat(s.status()).isEqualTo(Status.PENDING);
        s = sample(s, 4, 21, BAD);
        assertThat(s.pendingSince()).isEqualTo(T.plusSeconds(21));
        assertThat(s.episode()).isNull();
        s = sample(s, 5, 26, BAD);
        s = sample(s, 6, 31, BAD);
        s = sample(s, 7, 36, GOOD);
        s = sample(s, 8, 40, UNKNOWN);
        assertThat(s.status()).isEqualTo(Status.FIRING);
        assertThat(s.recoverySince()).isNull();
        s = sample(s, 9, 41, GOOD);
        assertThat(s.status()).isEqualTo(Status.FIRING);
    }
    @Test void recoveryAndRecurrenceNeedOnlyFreshEvidenceAndPreserveEndedHistory() {
        AlertSnapshot active = sample(AlertSnapshot.initial(rule(1, 0, 0)), 1, 0, BAD);
        AlertUpdate recovery = AlertLifecycle.evaluate(active, signal(active.rule(), 2, 3, GOOD, AlarmSeverity.MAJOR), T.plusSeconds(3));
        AlertSnapshot recovered = recovery.snapshot();
        Episode first = recovered.episode();
        assertThat(recovered.status()).isEqualTo(Status.RESOLVED);
        assertThat(first.endReason()).isEqualTo(EndReason.RECOVERED);
        assertThat(first.recoveredAt()).isEqualTo(T.plusSeconds(3));
        assertThat(recovery.transitions()).extracting(AlertUpdate.Transition::kind).containsExactly(AlertUpdate.Kind.RESOLVED);
        AlertSnapshot next = sample(recovered, 3, 4, BAD);
        assertThat(next.status()).isEqualTo(Status.FIRING);
        assertThat(next.episode().number()).isEqualTo(2);
        assertThat(next.episode().firedAt()).isEqualTo(T.plusSeconds(4));
        assertThat(next.episode().recoveredAt()).isNull();
        assertThat(next.episode().endedAt()).isNull();
        assertThat(recovered.episode()).isSameAs(first);
        assertThat(first.recoveredAt()).isEqualTo(T.plusSeconds(3));
        assertThat(sample(next, 2, 3, GOOD)).isEqualTo(next);
        assertThat(sample(next, 4, 3, GOOD)).isEqualTo(next);
    }
    @Test void severityChangesPreserveEpisodeAndBindingsRejectOldCallbacks() {
        AlertSnapshot s = sample(AlertSnapshot.initial(rule(1, 0, 0)), 1, 0, BAD);
        AlertUpdate update = AlertLifecycle.evaluate(s, signal(s.rule(), 2, 2, BAD, AlarmSeverity.CRITICAL), T.plusSeconds(2));
        s = update.snapshot();
        assertThat(s.episode().number()).isEqualTo(1);
        assertThat(s.episode().firedAt()).isEqualTo(T);
        assertThat(s.episode().severity()).isEqualTo(AlarmSeverity.CRITICAL);
        assertThat(s.episode().recoveredAt()).isNull();
        assertThat(update.transitions()).extracting(AlertUpdate.Transition::kind).containsExactly(AlertUpdate.Kind.SEVERITY_CHANGED);
        AlertSignal old = signal(s.rule(), 999, 3, GOOD, AlarmSeverity.MAJOR);
        s = AlertLifecycle.replace(s, rule(2, 0, 0), T.plusSeconds(3)).snapshot();
        assertThat(s.episode().endReason()).isEqualTo(EndReason.SUPERSEDED);
        assertThat(s.episode().recoveredAt()).isNull();
        assertThat(AlertLifecycle.evaluate(s, old, T.plusSeconds(3)).ignored()).isTrue();
        s = sample(s, 1, 4, BAD);
        assertThat(s.episode().number()).isEqualTo(2);
        assertThat(key("org-b", "resource-1", "rule-a")).isNotEqualTo(s.rule().key());
        assertThat(key("org-a", "resource-2", "rule-a")).isNotEqualTo(s.rule().key());
        assertThat(key("org-a", "resource-1", "rule-b")).isNotEqualTo(s.rule().key());
    }
    @Test void managementEndNeverClaimsBusinessRecoveryOrAcceptsMoreSignals() {
        for (EndReason reason : new EndReason[]{EndReason.DISABLED, EndReason.SUPERSEDED, EndReason.RESOURCE_REMOVED}) {
            AlertSnapshot s = sample(AlertSnapshot.initial(rule(1, 0, 0)), 1, 0, BAD);
            s = AlertLifecycle.terminate(s, reason, T.plusSeconds(1)).snapshot();
            assertThat(s.enabled()).isFalse();
            assertThat(s.episode().recoveredAt()).isNull();
            assertThat(s.episode().endedAt()).isEqualTo(T.plusSeconds(1));
            assertThat(sample(s, 2, 2, GOOD)).isEqualTo(s);
        }
    }
    @Test void invalidFutureInputDoesNotPoisonFrontierAndOldTimeCannotRefreshIt() {
        AlertSnapshot s = sample(AlertSnapshot.initial(rule(1, 0, 0)), 1, 0, BAD);
        s = AlertLifecycle.evaluate(s, signal(s.rule(), 100, 1000, GOOD, AlarmSeverity.MAJOR), T.plusSeconds(1)).snapshot();
        assertThat(s.quality()).isEqualTo(Quality.CLOCK_INVALID);
        assertThat(s.lastSequence()).isEqualTo(1);
        assertThat(s.status()).isEqualTo(Status.FIRING);
        s = sample(s, 2, 2, GOOD);
        assertThat(s.status()).isEqualTo(Status.RESOLVED);
        AlertSnapshot current = s;
        assertThat(AlertLifecycle.evaluate(s, signal(s.rule(), 3, 1, BAD, AlarmSeverity.MAJOR), T.plusSeconds(3)).snapshot()).isEqualTo(current);
    }
    @Test void wrongScopeResourceQualifierAndProducerCannotChangeAnActiveEpisode() {
        AlertSnapshot current = sample(AlertSnapshot.initial(rule(1, 0, 0)), 1, 0, BAD);
        for (AlarmKey foreign : new AlarmKey[]{key("org-b", "resource-1", "rule-a"),
                key("org-a", "resource-2", "rule-a"), key("org-a", "resource-1", "rule-b")}) {
            AlertSignal wrong = new AlertSignal(foreign, 1, 1, "producer-1", 2, T.plusSeconds(1), GOOD, AlarmSeverity.MAJOR);
            assertThat(AlertLifecycle.evaluate(current, wrong, T.plusSeconds(1)).snapshot()).isEqualTo(current);
        }
        AlertSignal wrongProducer = new AlertSignal(current.rule().key(), 1, 1, "another-producer", 2,
                T.plusSeconds(1), GOOD, AlarmSeverity.MAJOR);
        assertThat(AlertLifecycle.evaluate(current, wrongProducer, T.plusSeconds(1)).snapshot()).isEqualTo(current);
    }
}
