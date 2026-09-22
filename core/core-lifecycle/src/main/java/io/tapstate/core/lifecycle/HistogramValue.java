package io.tapstate.core.lifecycle;

import java.util.List;
import java.util.Objects;

/**
 * The distribution carried by a histogram point: how many observations fell in each bucket, plus the
 * total count and sum that let a consumer recover a mean without the buckets.
 *
 * <p>{@code bounds} are the explicit upper bounds, ascending and exclusive of the last bucket, so
 * {@code bucketCounts} always holds exactly one more entry than {@code bounds} — the final entry is
 * everything above the highest bound. That off-by-one is the whole shape of a bucketed distribution
 * and is checked here rather than trusted, because a consumer that receives equal-length lists has no
 * way to tell whether the overflow bucket was dropped or never existed, and either way it reports a
 * distribution missing its tail. The tail is the part anybody looking at a latency distribution came
 * for.
 *
 * <p>Bounds are not defaulted. A distribution whose buckets were chosen by whatever library happened
 * to be in the path measures the library's idea of the quantity's range, not ours, and the mismatch
 * shows up as every observation landing in one bucket — which reads exactly like a quantity that
 * never varies.
 */
public record HistogramValue(long count, double sum, List<Double> bounds, List<Long> bucketCounts) {

    public HistogramValue {
        Objects.requireNonNull(bounds, "bounds");
        Objects.requireNonNull(bucketCounts, "bucketCounts");
        if (count < 0) {
            throw new IllegalArgumentException("histogram count must not be negative: " + count);
        }
        if (bucketCounts.size() != bounds.size() + 1) {
            throw new IllegalArgumentException(
                    "a bucketed distribution carries one more count than it has bounds (the overflow "
                            + "bucket): bounds=" + bounds.size() + " bucketCounts=" + bucketCounts.size());
        }
        for (int i = 1; i < bounds.size(); i++) {
            if (bounds.get(i) <= bounds.get(i - 1)) {
                throw new IllegalArgumentException(
                        "bucket bounds must ascend strictly; bound " + i + " (" + bounds.get(i)
                                + ") does not exceed its predecessor (" + bounds.get(i - 1) + ")");
            }
        }
        bounds = List.copyOf(bounds);
        bucketCounts = List.copyOf(bucketCounts);
    }
}
