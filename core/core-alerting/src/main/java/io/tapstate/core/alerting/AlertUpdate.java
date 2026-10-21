package io.tapstate.core.alerting;

import java.time.Instant;
import java.util.List;

/** An atomic commit candidate. Computing it never acknowledges an input or sends a notification. */
public record AlertUpdate(AlertSnapshot snapshot, List<Transition> transitions, boolean ignored) {
    public enum Kind { PENDING, FIRING, RESOLVED, SEVERITY_CHANGED, ACKNOWLEDGED, ADMINISTRATIVELY_ENDED, QUALITY_CHANGED }
    public record Transition(Kind kind, Instant at, long episode, AlarmSeverity severity) { }
    public AlertUpdate { transitions = List.copyOf(transitions); }
}
