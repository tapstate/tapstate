package io.tapstate.core.lifecycle;

import java.time.Duration;
import java.util.Objects;

/**
 * How long a chain's durable position may stay pinned before it is worth saying so.
 *
 * <p>What a pinned position costs is not visible anywhere else. Nothing fails and no other statistic
 * moves: the pipeline stays running, its error count stays zero, its queues stay short and its throughput
 * is whatever it was — a chain can be pinned while the pipeline is busily processing every other chain it
 * has. What is happening meanwhile is that the source's read position cannot be advanced past the pinned
 * one, so the source keeps the log from there onwards, and a log has a retention. When it rotates past
 * that position the pipeline has not failed either; it has simply lost the ability to resume, and the
 * source must be rebuilt from scratch along with every pipeline mining the same chain.
 *
 * <p>So the threshold is set against a retention window rather than against anything the pipeline does.
 * It has to sit far enough above the ordinary waits that a healthy pipeline never reaches it — a child
 * row waiting for its parent is pinned, and that is a normal shape lasting seconds or minutes — and far
 * enough below the shortest retention anyone runs that there is still time to act when it fires.
 *
 * @param pinnedFor how long one chain may have its durable position pinned before it is reported
 */
public record FrontierStallPressure(Duration pinnedFor) {

    /**
     * A minute pinned before an on-demand explanation calls a chain stopped. This excludes the ordinary
     * millisecond-scale pauses sampled between advances while retaining the established minute-scale
     * diagnostic. The persistent alert stays deliberately less sensitive through {@link #DEFAULT}.
     */
    public static final FrontierStallPressure EXPLAIN = new FrontierStallPressure(Duration.ofMinutes(1));

    /**
     * An hour pinned. Provisional, and provisional in a specific way: the number that would justify it is
     * the deployment's own log retention, which the product does not know — the retention configured on a
     * source is carried as an opaque string and enforced by nobody here. An hour is chosen as two orders
     * of magnitude above the waits a healthy pipeline has (a parent row arriving late) and more than an
     * order of magnitude below the shortest retention these deployments run (a day), so it is wrong in the
     * direction that leaves time rather than the one that fires on working pipelines.
     */
    public static final FrontierStallPressure DEFAULT = new FrontierStallPressure(Duration.ofHours(1));

    public FrontierStallPressure {
        Objects.requireNonNull(pinnedFor, "pinnedFor");
        if (pinnedFor.isNegative() || pinnedFor.isZero()) {
            throw new IllegalArgumentException("pinnedFor must be a positive duration: " + pinnedFor);
        }
    }

    /**
     * Whether {@code stall} has been pinned long enough to be worth reporting. The threshold is met from
     * below by a continuous quantity, so reaching it exactly is reaching it.
     */
    public boolean isOver(FrontierStall stall) {
        return isOver(stall.pinnedMillis());
    }

    /** Whether a raw pinned-for reading has reached this pressure. */
    public boolean isOver(long pinnedMillis) {
        return pinnedMillis >= pinnedFor.toMillis();
    }
}
