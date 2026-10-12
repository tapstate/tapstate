package io.tapstate.e2e;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Conditional unrounded nominal bounds; fractional conversion enclosure is not physical clock qualification. */
final class BenchmarkNativeNominalReturnClock implements BenchmarkReturnPointClock {
    static final long SERIAL_MAPPING_TOLERANCE_NANOS = 2_000_000L;
    private final BenchmarkNativeReturnClock integerClock;
    private final List<BenchmarkCausalClock.Sample> samples;
    private final BenchmarkCausalClock.Identity root;

    BenchmarkNativeNominalReturnClock(BenchmarkCausalClock.Identity owned, BenchmarkCausalClock.Identity root,
            String library, List<BenchmarkCausalClock.Sample> samples,
            Map<String, Object> before, Map<String, Object> after) {
        if (samples == null || samples.size() < 2 || samples.size() > BenchmarkCausalClock.MAX_SAMPLES) {
            throw new AssertionError("nominal counter requires a bounded sample roster");
        }
        List<BenchmarkCausalClock.Sample> snapshot;
        try { snapshot = List.copyOf(samples); }
        catch (NullPointerException corrupt) { throw new AssertionError("nominal counter sample roster contains a missing read", corrupt); }
        integerClock = new BenchmarkNativeReturnClock(owned, root, library, snapshot, before, after);
        this.samples = snapshot;
        this.root = root;
    }

    void requireCaptureSamples(List<BenchmarkCausalClock.Sample> captured) {
        if (!samples.equals(captured)) {
            throw new AssertionError("nominal counter capture sample roster differs from its validated mapping");
        }
    }

    /** Checks point-mapping enclosures; request service time and literal-return width are separate facts. */
    Map<String, Object> serialCalibrationEvidence(List<BenchmarkCausalClock.Sample> captured) {
        return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.VERIFY_PHASE,
                () -> serialCalibrationEvidenceBounded(captured));
    }

    private Map<String, Object> serialCalibrationEvidenceBounded(List<BenchmarkCausalClock.Sample> captured) {
        if (captured == null || captured.size() < 2 || captured.size() > BenchmarkCausalClock.MAX_SAMPLES) {
            throw new AssertionError("nominal calibration lacks its bounded captured sample roster");
        }
        requireCaptureSamples(captured);
        long maximumMappingWidth = 0;
        long maximumRequestWidth = 0;
        for (var sample : samples) {
            // The unchanged delegate already checked the exact identity, cold route and strict integer bracket.
            // The common route maps this read's integer floor to the same root coordinate, with one fractional unit.
            var point = BenchmarkNominalCounterEnclosures.point(sample.ownedNanos());
            long width = point.widthNanos();
            if (width > SERIAL_MAPPING_TOLERANCE_NANOS) {
                throw new AssertionError("nominal serial point mapping exceeds its fixed tolerance");
            }
            maximumMappingWidth = Math.max(maximumMappingWidth, width);
            maximumRequestWidth = Math.max(maximumRequestWidth,
                    Math.subtractExact(sample.driverAfterNanos(), sample.driverBeforeNanos()));
        }
        var owned = samples.getFirst().identity();
        var result = new LinkedHashMap<String, Object>();
        result.put("state", "SAME_DOMAIN_SERIAL_MAPPING_ENCLOSURES_VERIFIED");
        result.put("calibrationObject", "OWNED_POINT_TO_BOUND_ROOT_NOMINAL_COORDINATE");
        result.put("rootPid", root.pid()); result.put("rootJvmStartTimeMillis", root.jvmStartTimeMillis());
        result.put("ownedPid", owned.pid()); result.put("ownedJvmStartTimeMillis", owned.jvmStartTimeMillis());
        result.put("samples", samples.size());
        result.put("pointEnclosure", "[q,q+1]");
        result.put("mappingUncertaintyUpperNominalNanos", maximumMappingWidth);
        result.put("mappingToleranceNominalNanos", SERIAL_MAPPING_TOLERANCE_NANOS);
        result.put("maximumActualRequestBracketNominalNanos", maximumRequestWidth);
        result.put("requestBracketScope", "SERVICE_AND_SCHEDULING_ENCLOSURE_NOT_MAPPING_ERROR");
        result.put("literalReturnScope", "CALLBACK_EXIT_TO_AFTER_RETURN_INTERVAL_NOT_MAPPING_ERROR");
        var binding = integerClock.evidence();
        result.put("boundRoute", binding.get("snapshot"));
        result.put("conditions", binding.get("conditions"));
        BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> result.put(flag, false));
        return Map.copyOf(result);
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
