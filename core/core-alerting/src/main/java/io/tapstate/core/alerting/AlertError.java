package io.tapstate.core.alerting;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import java.util.Set;

/** Diagnosable contract refusals; these are separate from alarm types and alarm severity. */
public enum AlertError implements TapstateErrorCode {
    INVALID_CONFIGURATION("alert.invalid-configuration", Set.of("field", "reason")),
    CHANNEL_NOT_ACCEPTED("alert.channel-not-accepted", Set.of("channel", "phase")),
    CHANNEL_RESULT_UNKNOWN("alert.channel-result-unknown", Set.of("channel", "phase")),
    CHANNEL_UNAVAILABLE("alert.channel-unavailable", Set.of("channel", "phase")),
    STALE_ACTION("alert.stale-action", Set.of("action"));

    private final String code;
    private final Set<String> placeholders;
    AlertError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }
    @Override public String code() { return code; }
    @Override public Severity severity() { return Severity.ERROR; }
    @Override public Set<String> placeholders() { return placeholders; }
}
