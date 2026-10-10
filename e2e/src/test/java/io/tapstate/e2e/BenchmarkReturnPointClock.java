package io.tapstate.e2e;

/** Maps one captured owned point into conservative driver-coordinate bounds. */
interface BenchmarkReturnPointClock {
    BenchmarkCausalClock.Interval map(BenchmarkCausalClock.Identity identity, long pointNanos);

    /** Existing driver timestamps retain their exact integer-coordinate interpretation. */
    default BenchmarkCausalClock.Interval sourcePoint(long pointNanos) {
        return new BenchmarkCausalClock.Interval(pointNanos, pointNanos);
    }
}
