package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertStateRecord.*;
import java.time.*;

final class AlertStateEnvelopeFixtures {
    static final Instant T = Instant.parse("2026-10-27T00:00:00.123456789Z");
    static final AlarmKey KEY = new AlarmKey(new AlarmKey.Scope("org-a", "cluster-a"),
            new AlarmKey.Resource("pipeline", "orders", "resource-1"), "pipeline.failed", "rule-a");
    static final AlertRule RULE = new AlertRule(KEY, 1, 2, "producer-1", Duration.ZERO, Duration.ZERO, Duration.ofSeconds(15), Duration.ofSeconds(10));
    static final AlertIntent.Policy POLICY = AlertIntent.Policy.capture(AlertRetentionPolicy.defaults(AlertChannelPolicy.Deployment.ON_PREM));
    static AlertSignal bad(long sequence, Instant at) { return new AlertSignal(KEY, 1, 2, "producer-1", sequence, at, AlertSignal.Condition.BAD, AlarmSeverity.MAJOR); }
    static Head fired() {
        var s = AlertLifecycle.evaluate(AlertSnapshot.initial(RULE), bad(1, T), T).snapshot();
        var d = Diagnostics.captured(AlarmDiagnostics.attach(s, bad(1, T), new AlarmDiagnostics.SafeTrace("java.lang.Exception\n\tat app.run(App.java:42)", 1, true, false), null).orElseThrow());
        return head(s, new Episode(RULE, s.episode(), POLICY, d, false), AlertOwnership.unclaimed());
    }
    static Head head(AlertSnapshot s, Episode e, AlertOwnership owner) { return new Head(s, POLICY.deployment(), owner, null, false, e); }
    static Clock clock(Instant now) { return Clock.fixed(now, ZoneOffset.UTC); }
}
