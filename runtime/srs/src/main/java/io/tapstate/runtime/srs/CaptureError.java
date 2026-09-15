package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

import java.util.Set;

/**
 * The {@code capture} domain's error codes — user-facing, diagnosable faults raised while wiring a
 * pipeline's consumption of a source's snapshot / cdc into the replay store. These are runtime faults
 * on values the authoring layer does not constrain, so they surface here rather than at validate time.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for each
 * name, and the build-time placeholder gate checks the message catalog templates against it. Bounded
 * control flow (a refused headroom write, a frontier that cannot yet advance) and connector read faults
 * (already coded {@code connector.*} by the pdk bridge) are deliberately not here — only genuine
 * capture-configuration errors are.
 */
public enum CaptureError implements TapstateErrorCode {

    /** A {@code start_from} value that is neither the {@code earliest} / {@code latest} keyword nor a
     *  parseable ISO-8601 instant; {@code value} carries the offending token. */
    START_FROM_UNPARSABLE("capture.start-from-unparsable", Set.of("value")),

    /**
     * A start_from instant that the replay buffer can no longer reach: it is older than every change
     * the buffer still holds. Starting at the buffer's head instead would come up healthy and stream a
     * different stretch than the one asked for, so this refuses instead. requested is the instant asked
     * for, earliest the oldest one still held, and retention the setting that decides how far back that
     * goes -- a reader needs all three to act on it.
     */
    START_FROM_OUTSIDE_WINDOW(
            "capture.start-from-outside-window", Set.of("requested", "earliest", "retention")),

    /** A connector emitted a table outside the selected capture streams; {@code table} is its name. */
    EVENT_TABLE_NOT_SELECTED("capture.event-table-not-selected", Set.of("table")),

    /**
     * The snapshot read reported no position for its change tail to pick up from, so the run stops.
     * {@code chain} is the mining chain being snapshotted. Carrying on would put the tail somewhere the
     * source never was, and every change made while the snapshot ran would be missed silently — which is
     * why this refuses rather than choosing a start of its own.
     */
    SNAPSHOT_REPORTS_NO_SEAM("capture.snapshot-reports-no-seam", Set.of("chain"));

    private final String code;
    private final Set<String> placeholders;

    CaptureError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
