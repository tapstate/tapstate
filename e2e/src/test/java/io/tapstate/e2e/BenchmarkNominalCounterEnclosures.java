package io.tapstate.e2e;

/** Bounds an unrounded nominal conversion using its recorded integer floor coordinate. */
final class BenchmarkNominalCounterEnclosures {
    private BenchmarkNominalCounterEnclosures() { }

    static BenchmarkCausalClock.Interval point(long floorNanos) {
        requirePoint(floorNanos);
        return new BenchmarkCausalClock.Interval(floorNanos, successor(floorNanos));
    }

    /** The actual return remains between the same call's callback-tail and post-return reads. */
    static BenchmarkCausalClock.Interval literalReturn(long callbackExitFloor, long afterReturnFloor) {
        requirePoint(callbackExitFloor); requirePoint(afterReturnFloor);
        if (callbackExitFloor > afterReturnFloor) {
            throw new AssertionError("nominal return read order is invalid");
        }
        return new BenchmarkCausalClock.Interval(callbackExitFloor, successor(afterReturnFloor));
    }

    static BenchmarkCausalClock.Interval sourceLatency(long sourceFloor, long callbackExitFloor, long afterReturnFloor) {
        return BenchmarkCausalClock.elapsed(point(sourceFloor), literalReturn(callbackExitFloor, afterReturnFloor));
    }

    private static void requirePoint(long value) {
        if (value < 0) { throw new AssertionError("nominal floor coordinate is outside its unsigned signed-long bound"); }
    }

    private static long successor(long value) {
        try { return Math.addExact(value, 1); }
        catch (ArithmeticException overflow) { throw new AssertionError("nominal conversion enclosure overflow", overflow); }
    }
}
