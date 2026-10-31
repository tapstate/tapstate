package io.tapstate.core.alerting;

import io.tapstate.core.common.TapstateException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.alerting.AlertChannelPolicy.Deployment.*;
import static io.tapstate.core.alerting.AlertSignal.Condition.*;
import static org.assertj.core.api.Assertions.*;

class AlarmDiagnosticsTest {
    static final Instant T = Instant.parse("2026-10-23T00:00:00Z");
    static AlertRule rule() {
        return new AlertRule(new AlarmKey(new AlarmKey.Scope("org", "cluster"), new AlarmKey.Resource("pipeline", "orders", "resource-1"),
                "pipeline.failed", "rule-1"), 1, 1, "producer-1", Duration.ZERO, Duration.ZERO, Duration.ofSeconds(15), Duration.ofSeconds(10));
    }
    static AlertSignal signal(AlertRule rule, long sequence, int second, AlertSignal.Condition condition) {
        return new AlertSignal(rule.key(), rule.revision(), rule.generation(), rule.producerIncarnation(), sequence,
                T.plusSeconds(second), condition, AlarmSeverity.MAJOR);
    }
    static AlertSnapshot accept(AlertSnapshot old, AlertSignal signal) {
        return AlertLifecycle.evaluate(old, signal, signal.observedAt()).snapshot();
    }
    static AlarmDiagnostics.SafeTrace trace(String text) { return new AlarmDiagnostics.SafeTrace(text, 1, false, false); }
    @Test void originalSafeWhitespaceExceptionChainsAndInitFramesRemainPlainText() {
        String text = "ExampleException: [redacted]\r\n\tat a.Type.<init>(Type.java:42)\r\n"
                + "Caused by: OtherException\n\tat b.Driver.read(Driver.java:8)\n";
        var safe = new AlarmDiagnostics.SafeTrace(text, 2, true, false);
        assertThat(safe.text()).isEqualTo(text);
        assertThat(safe.redacted()).isTrue(); assertThat(safe.truncated()).isFalse();
        assertThat(new AlarmDiagnostics.SafeTrace(text, 2, true, true).truncated()).isTrue();
    }
    @Test void utf8BoundaryNeverSplitsASupplementaryCodePointOrCrlf() {
        int max = AlarmDiagnostics.MAX_TEXT_BYTES;
        assertThat(trace("a".repeat(max)).truncated()).isFalse();
        var ascii = trace("a".repeat(max + 1));
        assertThat(ascii.text()).hasSize(max); assertThat(ascii.truncated()).isTrue();
        String face = "\uD83D\uDE00";
        var exact = trace("a".repeat(max - 4) + face);
        assertThat(exact.text().getBytes(StandardCharsets.UTF_8)).hasSize(max);
        assertThat(exact.truncated()).isFalse();
        var clipped = trace("a".repeat(max - 3) + face);
        assertThat(clipped.text()).isEqualTo("a".repeat(max - 3)); assertThat(clipped.truncated()).isTrue();
        assertThat(trace("a".repeat(max - 1) + "\r\n").text()).isEqualTo("a".repeat(max - 1));
    }
    @Test void lineAndNodeBudgetsIncludeTheirExactEndpointsAndPreserveTruncationEvidence() {
        String lines = "\tat example.Frame.call(Frame.java:1)\r\n".repeat(AlarmDiagnostics.MAX_LINES);
        assertThat(trace(lines).text()).isEqualTo(lines); assertThat(trace(lines).truncated()).isFalse();
        var more = new AlarmDiagnostics.SafeTrace(lines + "next", AlarmDiagnostics.MAX_EXCEPTION_NODES, false, false);
        assertThat(more.text()).isEqualTo(lines); assertThat(more.truncated()).isTrue();
        for (int count : new int[]{0, -1, AlarmDiagnostics.MAX_EXCEPTION_NODES + 1}) {
            assertThatThrownBy(() -> new AlarmDiagnostics.SafeTrace("Exception", count, false, false)).isInstanceOf(TapstateException.class);
        }
        assertThat(trace("x\u2028".repeat(AlarmDiagnostics.MAX_LINES) + "tail").text())
                .isEqualTo("x\u2028".repeat(AlarmDiagnostics.MAX_LINES));
    }
    @Test void malformedUnicodeAndUnexpectedControlsAreRejectedWithinTheBoundedPrefix() {
        for (String invalid : new String[]{"", "\uD800", "x\uDC00", "x\u0000y", "x\u001by"}) {
            assertThatThrownBy(() -> trace(invalid)).isInstanceOf(TapstateException.class);
        }
    }
    @Test void firstAndLatestRemainBoundedAndSameOccurrenceReplayCannotReplaceTheCapture() {
        var rule = rule(); var one = signal(rule, 1, 0, BAD);
        var state = accept(AlertSnapshot.initial(rule), one);
        var history = AlarmDiagnostics.attach(state, one, trace("FirstException"), null).orElseThrow();
        assertThat(history.first()).isSameAs(history.latest());
        var first = history.first(); var two = signal(rule, 2, 1, BAD); state = accept(state, two);
        history = AlarmDiagnostics.attach(state, two, trace("MiddleException"), history).orElseThrow();
        var three = signal(rule, 3, 2, BAD); state = accept(state, three);
        history = AlarmDiagnostics.attach(state, three, trace("LatestException"), history).orElseThrow();
        assertThat(history.first()).isSameAs(first);
        assertThat(history.latest().trace().text()).isEqualTo("LatestException");
        assertThat(history.latest().occurrence().sequence()).isEqualTo(3);
        assertThat(history.latest().occurrence().observedAt()).isEqualTo(T.plusSeconds(2));
        assertThat(AlarmDiagnostics.attach(state, three, trace("ChangedReplayException"), history).orElseThrow()).isSameAs(history);
        assertThat(AlarmDiagnostics.attach(state, two, trace("LateMiddleException"), history)).isEmpty();
    }
    @Test void onlyTheCurrentValidBadEvidenceCanAttachAndForeignBindingsAreNotGuessed() {
        var rule = rule(); var one = signal(rule, 1, 0, BAD); var state = accept(AlertSnapshot.initial(rule), one);
        var foreignKey = new AlarmKey(new AlarmKey.Scope("other", "cluster"), rule.key().resource(), rule.key().typeId(), rule.key().qualifier());
        for (var wrong : new AlertSignal[]{new AlertSignal(foreignKey, 1, 1, "producer-1", 1, T, BAD, AlarmSeverity.MAJOR),
                new AlertSignal(rule.key(), 2, 1, "producer-1", 1, T, BAD, AlarmSeverity.MAJOR),
                new AlertSignal(rule.key(), 1, 2, "producer-1", 1, T, BAD, AlarmSeverity.MAJOR),
                new AlertSignal(rule.key(), 1, 1, "producer-2", 1, T, BAD, AlarmSeverity.MAJOR),
                signal(rule, 2, 0, BAD), signal(rule, 1, 1, BAD), signal(rule, 1, 0, GOOD), signal(rule, 1, 0, UNKNOWN)}) {
            assertThat(AlarmDiagnostics.attach(state, wrong, trace("Exception"), null)).isEmpty();
        }
        assertThat(AlarmDiagnostics.attach(AlertLifecycle.tick(state, T.plusSeconds(16)).snapshot(), one, trace("Exception"), null)).isEmpty();
        assertThat(AlarmDiagnostics.attach(AlertLifecycle.terminate(state, AlertSnapshot.EndReason.DISABLED, T.plusSeconds(1)).snapshot(),
                one, trace("Exception"), null)).isEmpty();
    }
    @Test void recoveryAndNewEpisodesNeverReuseOldOccurrenceEvidence() {
        var rule = rule(); var one = signal(rule, 1, 0, BAD); var state = accept(AlertSnapshot.initial(rule), one);
        var history = AlarmDiagnostics.attach(state, one, trace("OldException"), null).orElseThrow();
        var good = signal(rule, 2, 1, GOOD); var recovered = accept(state, good);
        assertThat(AlarmDiagnostics.attach(recovered, one, trace("LateException"), history)).isEmpty();
        var three = signal(rule, 3, 2, BAD); var current = accept(recovered, three);
        assertThat(AlarmDiagnostics.attach(current, one, trace("LateException"), history)).isEmpty();
        assertThatThrownBy(() -> AlarmDiagnostics.attach(current, three, trace("NewException"), history)).isInstanceOf(IllegalStateException.class);
        var next = AlarmDiagnostics.attach(current, three, trace("NewException"), null).orElseThrow();
        assertThat(next.first().occurrence().episode()).isEqualTo(2);
        assertThat(history.first().trace().text()).isEqualTo("OldException");
    }
    @Test void diagnosticBindingSeparatesEveryEvaluationIdentityDimension() {
        var original = rule(); String digest = AlarmDiagnostics.bindingDigest(original);
        assertThat(AlarmDiagnostics.bindingDigest(rule())).isEqualTo(digest).matches("[0-9a-f]{64}");
        var key = original.key();
        var variants = new AlarmKey[]{new AlarmKey(new AlarmKey.Scope("other", "cluster"), key.resource(), key.typeId(), key.qualifier()),
                new AlarmKey(new AlarmKey.Scope("org", "other"), key.resource(), key.typeId(), key.qualifier()),
                new AlarmKey(key.scope(), new AlarmKey.Resource("source", "orders", "resource-1"), key.typeId(), key.qualifier()),
                new AlarmKey(key.scope(), new AlarmKey.Resource("pipeline", "other", "resource-1"), key.typeId(), key.qualifier()),
                new AlarmKey(key.scope(), new AlarmKey.Resource("pipeline", "orders", "resource-2"), key.typeId(), key.qualifier()),
                new AlarmKey(key.scope(), key.resource(), "pipeline.observation-stale", key.qualifier()),
                new AlarmKey(key.scope(), key.resource(), key.typeId(), "other-rule")};
        for (var changed : variants) {
            assertThat(AlarmDiagnostics.bindingDigest(new AlertRule(changed, 1, 1, "producer-1", Duration.ZERO, Duration.ZERO,
                    original.maxAge(), original.maxGap()))).isNotEqualTo(digest);
        }
        for (var changed : new AlertRule[]{new AlertRule(key, 2, 1, "producer-1", Duration.ZERO, Duration.ZERO, original.maxAge(), original.maxGap()),
                new AlertRule(key, 1, 2, "producer-1", Duration.ZERO, Duration.ZERO, original.maxAge(), original.maxGap()),
                new AlertRule(key, 1, 1, "producer-2", Duration.ZERO, Duration.ZERO, original.maxAge(), original.maxGap())}) {
            assertThat(AlarmDiagnostics.bindingDigest(changed)).isNotEqualTo(digest);
        }
    }
    @Test void historyCannotBeReconstructedWithMixedOrReorderedOccurrenceIdentities() {
        String binding = AlarmDiagnostics.bindingDigest(rule());
        var first = new AlarmDiagnostics.Capture(new AlarmDiagnostics.Occurrence(binding, 1, 1, T), trace("First"));
        for (var wrong : new AlarmDiagnostics.Occurrence[]{new AlarmDiagnostics.Occurrence("a".repeat(64), 1, 2, T.plusSeconds(1)),
                new AlarmDiagnostics.Occurrence(binding, 2, 2, T.plusSeconds(1)),
                new AlarmDiagnostics.Occurrence(binding, 1, 0, T.minusSeconds(1)),
                new AlarmDiagnostics.Occurrence(binding, 1, 2, T)}) {
            assertThatThrownBy(() -> new AlarmDiagnostics.History(first, new AlarmDiagnostics.Capture(wrong, trace("Wrong"))))
                    .isInstanceOf(TapstateException.class);
        }
        assertThatThrownBy(() -> new AlarmDiagnostics.History(first, new AlarmDiagnostics.Capture(first.occurrence(), trace("Changed"))))
                .isInstanceOf(TapstateException.class);
    }
    @Test void traceCutoffKeepsActiveEvidenceAndNeverOutlivesTheEndedEpisodePolicy() {
        var state = accept(AlertSnapshot.initial(rule()), signal(rule(), 1, 0, BAD));
        for (var deployment : new AlertChannelPolicy.Deployment[]{ON_PREM, CLOUD_MANAGED}) {
            var policy = AlertRetentionPolicy.defaults(deployment);
            assertThat(AlarmDiagnostics.cutoff(policy, state.episode())).isEmpty();
            var ended = accept(state, signal(rule(), 2, 1, GOOD)).episode();
            assertThat(AlarmDiagnostics.cutoff(policy, ended)).contains(T.plusSeconds(1).plus(Duration.ofDays(deployment == ON_PREM ? 7 : 1)));
            var values = new EnumMap<>(policy.values()); values.put(AlertRetentionPolicy.Setting.ENDED_EPISODE, Duration.ofDays(1));
            assertThat(AlarmDiagnostics.cutoff(AlertRetentionPolicy.configure(deployment, values), ended)).contains(T.plusSeconds(1).plus(Duration.ofDays(1)));
            var disabled = AlertLifecycle.terminate(state, AlertSnapshot.EndReason.DISABLED, T.plusSeconds(2)).snapshot().episode();
            assertThat(disabled.recoveredAt()).isNull();
            assertThat(AlarmDiagnostics.cutoff(policy, disabled)).contains(T.plusSeconds(2).plus(Duration.ofDays(deployment == ON_PREM ? 7 : 1)));
        }
    }
}
