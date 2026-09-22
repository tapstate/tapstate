package io.tapstate.runtime.engine.join;

import java.util.ArrayList;
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
 * Each control is a window of repeated runs. Its low eighth preserves the statistic that the fastest
 * of eight represented, while the larger population means a scheduler pause is not asked to move one
 * two-millisecond denominator by a useful fraction of itself.
 */
record JoinBenchComparison(JoinBenchComparison.Timing carrier,
                           JoinBenchComparison.Timing before,
                           JoinBenchComparison.Timing after) {

    static final int CONTROL_SAMPLES = 8;
    static final int HEAP_WARMUPS = 200;

    /**
     * The minimum timed work behind one normalized control sample.
     *
     * <p>The heap arm finishes in about two milliseconds. At that size, a delay of only one or two
     * milliseconds moves the denominator by half or all of itself. Summing one hundred milliseconds
     * of the same work supplies enough observations to keep the same low-eighth statistic without
     * handing the verdict to one short interval.
     */
    static final long CONTROL_WINDOW_NANOS = 100_000_000L;

    JoinBenchComparison {
        if (after.midpoint() - before.midpoint() <= 0
                || carrier.midpoint() - before.midpoint() < 0
                || after.midpoint() - carrier.midpoint() < 0) {
            throw new IllegalArgumentException("the controls must bracket the carrier's timed region");
        }
    }

    record Timing(long nanos, long midpoint, long windowNanos) {

        Timing(long nanos, long midpoint) {
            this(nanos, midpoint, nanos);
        }

        Timing {
            if (nanos <= 0) {
                throw new IllegalArgumentException("a timed sample must have positive duration");
            }
            if (windowNanos < nanos) {
                throw new IllegalArgumentException("a control window cannot be shorter than its "
                        + "normalized sample");
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

    static JoinBenchComparison measureWithControlWindow(Supplier<Timing> control,
            Supplier<Timing> carrier, int controlSamples) {
        return measureWithControlWindow(control, carrier, controlSamples, CONTROL_WINDOW_NANOS);
    }

    static JoinBenchComparison measureWithControlWindow(Supplier<Timing> control,
            Supplier<Timing> carrier, int controlSamples, long minimumWindowNanos) {
        if (controlSamples < 1) {
            throw new IllegalArgumentException("at least one control sample is required");
        }
        if (minimumWindowNanos < 1) {
            throw new IllegalArgumentException("a positive control window is required");
        }
        Timing before = window(control, controlSamples, minimumWindowNanos);
        Timing measured = carrier.get();
        Timing after = window(control, controlSamples, minimumWindowNanos);
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

    private static Timing window(Supplier<Timing> run, int minimumRuns, long minimumWindowNanos) {
        Timing first = run.get();
        long total = first.nanos();
        int runs = 1;
        List<Timing> timings = new ArrayList<>();
        timings.add(first);
        while (runs < minimumRuns || total < minimumWindowNanos) {
            Timing each = run.get();
            total += each.nanos();
            runs++;
            timings.add(each);
        }
        timings.sort(Comparator.comparingLong(Timing::nanos));
        Timing normalized = timings.get((timings.size() - 1) / minimumRuns);
        return new Timing(normalized.nanos(), normalized.midpoint(), total);
    }

    double controlNanos() {
        double position = (double) (carrier.midpoint() - before.midpoint())
                / (after.midpoint() - before.midpoint());
        return before.nanos() + position * (after.nanos() - before.nanos());
    }

    double ratio() {
        return carrier.nanos() / controlNanos();
    }

    long controlWindowNanos() {
        return Math.min(before.windowNanos(), after.windowNanos());
    }

    static JoinBenchComparison best(List<JoinBenchComparison> samples) {
        return samples.stream().min(Comparator.comparingDouble(JoinBenchComparison::ratio))
                .orElseThrow();
    }
}
