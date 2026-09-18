package io.tapstate.core.lifecycle;

/**
 * What kind of measurement a {@link MetricFact} carries, and therefore how a consumer may read it.
 * The type is part of the fact rather than something each reader infers, because the three are not
 * interchangeable and guessing wrong is silent: a rate taken over a gauge is meaningless, and a
 * counter read as a gauge loses the reset that tells a consumer its accumulation restarted.
 *
 * <ul>
 *   <li>{@link #COUNTER} — monotonically accumulating. Its points MUST carry a start, because an
 *       accumulating stream with no start identity cannot tell "the process restarted and the count
 *       began again" from "the count went backwards", and those two need opposite reactions.</li>
 *   <li>{@link #GAUGE} — a value read at a moment, free to go up and down. A latest-value reading
 *       with no accumulation behind it is this, whatever the underlying quantity does.</li>
 *   <li>{@link #HISTOGRAM} — a distribution: bucket counts plus their total and sum. A histogram
 *       stops producing observations when the thing it measures stops happening, so it cannot be
 *       used to detect a stall; that question belongs to a gauge.</li>
 * </ul>
 */
public enum MetricType {
    COUNTER,
    GAUGE,
    HISTOGRAM
}
