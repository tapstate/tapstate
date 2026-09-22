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
 * control flow (a refused headroom write, a ring operation the cluster refuses while its members'
 * verdicts converge, a frontier that cannot yet advance) and connector read faults (already coded
 * {@code connector.*} by the pdk bridge) are deliberately not here — only genuine capture-configuration
 * errors are. Where such a wait carries a bound, reaching that bound is a fault and is coded; being
 * inside it is not.
 */
public enum CaptureError implements TapstateErrorCode {

    /** A live capture could no longer renew the cluster ownership generation that fences its writes. */
    CLAIM_LOST("capture.claim-lost", Set.of("captureId")),

    /**
     * The cluster refused this member's writes into a change ring for the whole stretch the capture waits
     * such a refusal out. A refusal while members' verdicts converge is transient and is waited out rather
     * than coded; this is the one that never cleared, which says this member is not in a cluster that
     * qualifies to hold the work. {@code table} names the ring's table, {@code seconds} how long it waited.
     */
    CLUSTER_REFUSED_WRITES("capture.cluster-refused-writes", Set.of("table", "seconds")),

    /**
     * The cluster refused this member's reads of a change ring for the whole stretch a source waits such a
     * refusal out. The mirror of the one above, on the path a source takes to work out where it starts:
     * that reading is guarded like every other ring operation, and a forming cluster refuses it. Waiting is
     * the answer while the verdicts converge; this is the one that never cleared. {@code ring} names the
     * ring, {@code seconds} how long it waited.
     */
    CLUSTER_REFUSED_THE_READ("capture.cluster-refused-the-read", Set.of("ring", "seconds")),

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
