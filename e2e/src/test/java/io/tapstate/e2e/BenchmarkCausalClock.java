package io.tapstate.e2e;

import java.util.List;

/**
 * Maps an owned JVM's captured clock point using causal request bounds alone.
 * The supplied clock must preserve temporal order within that JVM. This mapper does not establish
 * its resolution, the delay between a write return and its clock read, or instrumentation cost.
 */
final class BenchmarkCausalClock implements BenchmarkReturnPointClock {
    static final int MAX_SAMPLES = 512;

    record Identity(long pid, long jvmStartTimeMillis) {
        Identity {
            if (pid <= 0 || jvmStartTimeMillis <= 0) {
                throw new AssertionError("causal clock needs an exact owned runtime identity");
            }
        }
    }

    /** The owned clock is read during this actual, serial driver request. */
    record Sample(long sequence, Identity identity, long driverBeforeNanos,
                  long driverAfterNanos, long ownedNanos) {
        Sample {
            if (identity == null || sequence < 0) {
                throw new AssertionError("causal clock sample has an invalid identity or sequence");
            }
            if (difference(driverAfterNanos, driverBeforeNanos, "request bracket") < 0) {
                throw new AssertionError("causal clock request bracket moved backward");
            }
        }
    }

    /** Closed bounds; negative coordinates and relative lower bounds remain meaningful evidence. */
    record Interval(long lowerNanos, long upperNanos) {
        Interval {
            if (difference(upperNanos, lowerNanos, "interval width") < 0) {
                throw new AssertionError("causal clock interval bounds moved backward");
            }
        }

        long widthNanos() {
            return difference(upperNanos, lowerNanos, "interval width");
        }

        /** Subtracts an actual driver point, without clipping uncertain negative latency bounds. */
        Interval relativeTo(long driverPointNanos) {
            return new Interval(difference(lowerNanos, driverPointNanos, "relative lower bound"),
                    difference(upperNanos, driverPointNanos, "relative upper bound"));
        }
    }

    private final Identity identity;
    private final List<Sample> samples;

    BenchmarkCausalClock(Identity expectedIdentity, List<Sample> readings) {
        if (expectedIdentity == null || readings == null || readings.size() < 2
                || readings.size() > MAX_SAMPLES) {
            throw new AssertionError("causal clock needs its owned identity and bounded sample roster");
        }
        List<Sample> snapshot;
        try {
            snapshot = List.copyOf(readings);
        } catch (NullPointerException corrupt) {
            throw new AssertionError("causal clock sample has a missing owned runtime identity", corrupt);
        }
        if (snapshot.size() < 2 || snapshot.size() > MAX_SAMPLES) {
            throw new AssertionError("causal clock needs a bounded sample roster");
        }
        Sample previous = null;
        for (Sample sample : snapshot) {
            if (!expectedIdentity.equals(sample.identity())) {
                throw new AssertionError("causal clock sample has another or missing owned runtime identity");
            }
            if (previous != null) {
                if (sample.sequence() <= previous.sequence()) {
                    throw new AssertionError("causal clock sample sequence did not advance");
                }
                if (difference(sample.driverBeforeNanos(), previous.driverAfterNanos(),
                        "serial request order") < 0) {
                    throw new AssertionError("causal clock request brackets are not ordered serial requests");
                }
                if (difference(sample.ownedNanos(), previous.ownedNanos(), "owned counter order") < 0) {
                    throw new AssertionError("causal clock owned counter moved backward");
                }
            }
            previous = sample;
        }
        difference(snapshot.getLast().driverAfterNanos(), snapshot.getFirst().driverBeforeNanos(),
                "driver coverage span");
        difference(snapshot.getLast().ownedNanos(), snapshot.getFirst().ownedNanos(),
                "owned coverage span");
        identity = expectedIdentity;
        samples = snapshot;
    }

    List<Sample> samples() {
        return samples;
    }

    @Override public Interval map(Identity actualIdentity, long ownedPointNanos) {
        if (!identity.equals(actualIdentity)) {
            throw new AssertionError("causal clock point has another or missing owned runtime identity");
        }
        difference(ownedPointNanos, samples.getFirst().ownedNanos(), "owned point coverage span");
        difference(samples.getLast().ownedNanos(), ownedPointNanos, "owned point coverage span");
        Sample earlier = null;
        Sample later = null;
        for (Sample sample : samples) {
            if (sample.ownedNanos() < ownedPointNanos) {
                earlier = sample;
            } else if (sample.ownedNanos() > ownedPointNanos) {
                later = sample;
                break;
            }
        }
        if (earlier == null || later == null) {
            throw new AssertionError("causal clock point lacks strictly enclosing owned samples");
        }
        // The point is after the earlier getter and before the later getter. Their actual reads
        // are inside their driver brackets; no offset or rate interpolation narrows these bounds.
        return new Interval(earlier.driverBeforeNanos(), later.driverAfterNanos());
    }

    /** Independent interval subtraction; it does not assume that two mapped points are ordered. */
    static Interval elapsed(Interval start, Interval end) {
        if (start == null || end == null) {
            throw new AssertionError("causal clock elapsed bounds need both endpoints");
        }
        return new Interval(difference(end.lowerNanos(), start.upperNanos(), "elapsed lower bound"),
                difference(end.upperNanos(), start.lowerNanos(), "elapsed upper bound"));
    }

    private static long difference(long later, long earlier, String scope) {
        try {
            return Math.subtractExact(later, earlier);
        } catch (ArithmeticException overflow) {
            throw new AssertionError("causal clock " + scope + " overflow", overflow);
        }
    }
}
