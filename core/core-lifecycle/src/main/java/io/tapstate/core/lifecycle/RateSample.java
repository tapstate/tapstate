package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * One sample of a pipeline's movement, kept so that a rate can be drawn over time: the counters and the
 * per-table delay as they stood at one moment, and what the counters accumulate from.
 *
 * <p>A sample carries counters and readings, never a rate. A rate is a difference of two samples over the
 * difference of their times, and a sample that stored a rate would also have stored the window it was
 * taken over — permanently, for every reader, whatever window they wanted. Two neighbouring samples give
 * any window a reader asks for; that is the same arithmetic a command line does over two observations,
 * on the same numbers.
 *
 * <p>{@code counters} holds the pipeline-level totals the flat metrics face carries — records and bytes per
 * direction, errors per code, the driven count — and {@code lag} the per-table delay in seconds. Both are
 * a subset of that face on purpose: what is kept is what a line can be drawn from, and a sample is taken
 * once a minute for fifteen days, so every key in it is paid for twenty thousand times over.
 *
 * <p>{@code countingSince} says what the counters accumulate from. Two samples with different starts
 * straddle a restart, and a reader differencing them must notice that first: a total that dropped with the
 * start moved is a restart, and a total that dropped with the start still is a counter going backwards.
 * Absent when the run reported no start, which is a run that has counted nothing.
 */
public record RateSample(String pipelineId, Instant observedAt, Map<String, Long> counters,
        Map<String, Long> lag, Instant countingSince) {

    public RateSample {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(observedAt, "observedAt");
        counters = counters == null ? Map.of() : Map.copyOf(counters);
        lag = lag == null ? Map.of() : Map.copyOf(lag);
    }
}
