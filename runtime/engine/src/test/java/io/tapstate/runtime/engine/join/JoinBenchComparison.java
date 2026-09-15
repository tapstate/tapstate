package io.tapstate.runtime.engine.join;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

/**
 * A carrier sample bracketed by controls from the same part of the run.
 *
 * <p>Interpolate the control cost at the carrier's timed midpoint. Setup and shutdown are unequal
 * and untimed, so the carrier is not necessarily halfway between the controls. This removes a
 * locally linear change in the common machine cost; it cannot remove arbitrary scheduling noise
 * or a slowdown that affects only the carrier. Repeated trials still take the least disturbed ratio.
 */
record JoinBenchComparison(JoinBenchComparison.Timing carrier,
                           JoinBenchComparison.Timing before,
                           JoinBenchComparison.Timing after) {

    static final int CONTROL_SAMPLES = 8;
    static final int HEAP_WARMUPS = 200;

    JoinBenchComparison {
        if (after.midpoint() - before.midpoint() <= 0
                || carrier.midpoint() - before.midpoint() < 0
                || after.midpoint() - carrier.midpoint() < 0) {
            throw new IllegalArgumentException("the controls must bracket the carrier's timed region");
        }
    }

    record Timing(long nanos, long midpoint) {
        Timing {
            if (nanos <= 0) {
                throw new IllegalArgumentException("a timed sample must have positive duration");
            }
        }
    }

    static JoinBenchComparison measure(Supplier<Timing> control, Supplier<Timing> carrier,
            int controlSamples) {
        if (controlSamples < 1) {
            throw new IllegalArgumentException("at least one control sample is required");
        }
        Timing before = fastest(control, controlSamples);
        Timing measured = carrier.get();
        Timing after = fastest(control, controlSamples);
        return new JoinBenchComparison(measured, before, after);
    }

    private static Timing fastest(Supplier<Timing> run, int samples) {
        Timing best = run.get();
        for (int i = 1; i < samples; i++) {
            Timing each = run.get();
            if (each.nanos() < best.nanos()) {
                best = each;
            }
        }
        return best;
    }

    double controlNanos() {
        double position = (double) (carrier.midpoint() - before.midpoint())
                / (after.midpoint() - before.midpoint());
        return before.nanos() + position * (after.nanos() - before.nanos());
    }

    double ratio() {
        return carrier.nanos() / controlNanos();
    }

    static JoinBenchComparison best(List<JoinBenchComparison> samples) {
        return samples.stream().min(Comparator.comparingDouble(JoinBenchComparison::ratio))
                .orElseThrow();
    }
}
