package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Objects;

/**
 * How one namespace's operator state has been made durable since its counting began: how many commit groups
 * there were, how many keys each carried, how many store round trips each cost and how long each took; and how
 * many entries memory let go of.
 *
 * <p>A commit group is one acknowledged store write call, whatever the layer above batched into it. That is the
 * unit the store pays for - validation, fencing and the write itself - so it is the unit a reader divides by:
 * keys per group says how well writes are amortized, round trips per group what each group costs, and the
 * two together give round trips per key.
 *
 * <p>Everything here accumulates from {@code countingSince}, so a restart is a new series rather than a
 * decrease.
 *
 * @param groups how many commit groups became durable
 * @param keys keys per group, over the registered bounds
 * @param roundTrips store round trips per group, over the registered bounds
 * @param duration seconds per group, over the registered bounds
 * @param evictions how many entries memory let go of, to be read back from the store when next asked for
 * @param countingSince when these counts began
 */
public record StateCommitReading(long groups, HistogramValue keys, HistogramValue roundTrips,
        HistogramValue duration, long evictions, Instant countingSince) {

    public StateCommitReading {
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(roundTrips, "roundTrips");
        Objects.requireNonNull(duration, "duration");
        Objects.requireNonNull(countingSince, "countingSince");
        if (groups < 0 || evictions < 0) {
            throw new IllegalArgumentException("commit and eviction counts must not be negative");
        }
    }
}
