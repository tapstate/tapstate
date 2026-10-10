package io.tapstate.e2e;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Conditional unrounded nominal bounds; fractional conversion enclosure is not physical clock qualification. */
final class BenchmarkNativeNominalReturnClock implements BenchmarkReturnPointClock {
    private final BenchmarkNativeReturnClock integerClock;

    BenchmarkNativeNominalReturnClock(BenchmarkCausalClock.Identity owned, BenchmarkCausalClock.Identity root,
            String library, List<BenchmarkCausalClock.Sample> samples,
            Map<String, Object> before, Map<String, Object> after) {
        integerClock = new BenchmarkNativeReturnClock(owned, root, library, samples, before, after);
    }

    @Override public BenchmarkCausalClock.Interval map(BenchmarkCausalClock.Identity identity, long pointNanos) {
        var point = integerClock.map(identity, pointNanos);
        return BenchmarkNominalCounterEnclosures.point(point.lowerNanos());
    }

    @Override public BenchmarkCausalClock.Interval sourcePoint(long pointNanos) {
        return BenchmarkNominalCounterEnclosures.point(pointNanos);
    }

    Map<String, Object> evidence() {
        var result = new LinkedHashMap<String, Object>(integerClock.evidence());
        result.put("integerBindingState", result.get("state"));
        result.put("state", "CONDITIONAL_UNROUNDED_NOMINAL_COUNTER");
        result.put("mapping", "RECORDED_INTEGER_FLOOR_TO_CHECKED_CLOSED_NOMINAL_ENCLOSURE");
        result.put("fractionalFloorUpperErrorNominalNanos", 1L);
        result.put("pointEnclosure", "[q,q+1]");
        result.put("sourcePointEnclosure", "[qS,qS+1]");
        result.put("literalReturn", "[qL,qU+1]; SAME_CALL_CALLBACK_EXIT_TO_AFTER_NORMAL_RETURN");
        result.put("sourceLatency", "[qL-(qS+1),(qU+1)-qS]");
        result.put("errorScope", "FRACTIONAL_INTEGER_FLOOR_ONLY; NATIVE_READ_AND_RETURN_ORDER_CONDITIONS_RETAINED");
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> result.put(flag, false));
        return Map.copyOf(result);
    }
}
