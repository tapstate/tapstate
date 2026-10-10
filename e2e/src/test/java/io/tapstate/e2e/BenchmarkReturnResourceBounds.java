package io.tapstate.e2e;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Counter enclosures from actual read brackets; identity, continuous memory peaks and sampler cost remain separate. */
final class BenchmarkReturnResourceBounds {
    private static final int MAX_ROWS = 512 * 512;
    private static final int MAX_ATTEMPTS = 4096;

    private BenchmarkReturnResourceBounds() { }

    static Map<String, Object> evidence(BenchmarkResourceSampler.Summary full,
                                        List<BenchmarkReturnTimeBounds.Delivery> completeCohort) {
        if (full == null || full.sampling().isEmpty()) { return unknown("FULL_RESOURCE_TRACE_UNAVAILABLE"); }
        if (completeCohort == null || completeCohort.isEmpty() || completeCohort.size() > MAX_ROWS) {
            return unknown("COMPLETE_COHORT_UNAVAILABLE");
        }
        var trace = full.sampling().orElseThrow();
        if (trace.omittedAttempts() != 0 || trace.failureCount() != 0 || !"COMPLETE".equals(trace.state())) {
            return unknown("RESOURCE_TRACE_HAS_OMITTED_OR_FAILED_READS");
        }
        var attempts = trace.retainedAttempts();
        if (attempts.size() < 2 || attempts.size() > MAX_ATTEMPTS || trace.attemptCount() != attempts.size()
                || full.sampleCount() != attempts.size() || trace.retainedAttemptLimit() < attempts.size()
                || trace.retainedAttemptLimit() > MAX_ATTEMPTS) {
            return unknown("RESOURCE_TRACE_ROSTER_CONTRADICTION");
        }
        long startLower = Long.MAX_VALUE, startUpper = Long.MAX_VALUE;
        long endLower = Long.MIN_VALUE, endUpper = Long.MIN_VALUE;
        for (var delivery : completeCohort) {
            if (delivery == null || delivery.literalReturn() == null) { return unknown("RETURN_WINDOW_UNAVAILABLE"); }
            var returned = delivery.literalReturn();
            startLower = Math.min(startLower, returned.lowerNanos());
            startUpper = Math.min(startUpper, returned.upperNanos());
            endLower = Math.max(endLower, returned.lowerNanos());
            endUpper = Math.max(endUpper, returned.upperNanos());
        }
        BenchmarkResourceSampler.Attempt previous = null;
        long totalReadNanos = 0, maximumReadNanos = 0, fullHeap = 0, fullRss = 0;
        try {
            for (int index = 0; index < attempts.size(); index++) {
                var attempt = attempts.get(index);
                if (attempt == null || attempt.index() != index + 1L
                        || attempt.outcome() != BenchmarkResourceSampler.Outcome.SUCCESS
                        || attempt.failureType() != null || attempt.reading() == null || !attempt.reading().complete()) {
                    return unknown("RESOURCE_READ_SHAPE_UNAVAILABLE");
                }
                var reading = attempt.reading();
                if (reading.cpuNanos().orElseThrow() < 0 || reading.gcCollectionMillis().orElseThrow() < 0
                        || reading.heapUsedBytes().orElseThrow() <= 0 || reading.rssBytes().orElseThrow() <= 0) {
                    return unknown("RESOURCE_COUNTER_OR_MEMORY_UNAVAILABLE");
                }
                long duration = Math.subtractExact(attempt.completedAtNanos(), attempt.startedAtNanos());
                if (duration < 0 || previous != null
                        && Math.subtractExact(attempt.startedAtNanos(), previous.completedAtNanos()) < 0) {
                    return unknown("RESOURCE_READ_BRACKETS_NOT_SERIAL");
                }
                if (previous != null && (reading.cpuNanos().orElseThrow() < previous.reading().cpuNanos().orElseThrow()
                        || reading.gcCollectionMillis().orElseThrow() < previous.reading().gcCollectionMillis().orElseThrow())) {
                    return unknown("CUMULATIVE_RESOURCE_COUNTER_MOVED_BACKWARD");
                }
                totalReadNanos = Math.addExact(totalReadNanos, duration);
                maximumReadNanos = Math.max(maximumReadNanos, duration);
                fullHeap = Math.max(fullHeap, reading.heapUsedBytes().orElseThrow());
                fullRss = Math.max(fullRss, reading.rssBytes().orElseThrow());
                previous = attempt;
            }
            var first = attempts.getFirst().reading(); var last = attempts.getLast().reading();
            if (Math.subtractExact(last.cpuNanos().orElseThrow(), first.cpuNanos().orElseThrow()) != full.cpuNanos()
                    || Math.subtractExact(last.gcCollectionMillis().orElseThrow(), first.gcCollectionMillis().orElseThrow())
                    != full.gcCollectionMillis() || fullHeap != full.peakHeapBytes() || fullRss != full.peakRssBytes()
                    || totalReadNanos != trace.totalDurationNanos() || maximumReadNanos != trace.maxDurationNanos()) {
                return unknown("FULL_RESOURCE_SUMMARY_CONTRADICTS_TRACE");
            }
        } catch (ArithmeticException overflow) { return unknown("RESOURCE_TRACE_ARITHMETIC_OVERFLOW"); }

        BenchmarkResourceSampler.Attempt outerFirst = null, outerLast = null, innerFirst = null, innerLast = null;
        int innerReads = 0, possibleReads = 0;
        long innerHeap = 0, innerRss = 0, possibleHeap = 0, possibleRss = 0;
        for (var attempt : attempts) {
            if (attempt.completedAtNanos() <= startLower) { outerFirst = attempt; }
            if (outerLast == null && attempt.startedAtNanos() >= endUpper) { outerLast = attempt; }
            if (attempt.startedAtNanos() >= startUpper && attempt.completedAtNanos() <= endLower) {
                if (innerFirst == null) { innerFirst = attempt; }
                innerLast = attempt;
                innerReads++;
                innerHeap = Math.max(innerHeap, attempt.reading().heapUsedBytes().orElseThrow());
                innerRss = Math.max(innerRss, attempt.reading().rssBytes().orElseThrow());
            }
            if (attempt.completedAtNanos() >= startLower && attempt.startedAtNanos() <= endUpper) {
                possibleReads++;
                possibleHeap = Math.max(possibleHeap, attempt.reading().heapUsedBytes().orElseThrow());
                possibleRss = Math.max(possibleRss, attempt.reading().rssBytes().orElseThrow());
            }
        }
        if (outerFirst == null || outerLast == null) { return unknown("OUTER_RESOURCE_READ_COVERAGE_MISSING"); }
        long cpuLower = innerReads < 2 ? 0 : difference(innerFirst, innerLast, true);
        long gcLower = innerReads < 2 ? 0 : difference(innerFirst, innerLast, false);
        long cpuUpper = difference(outerFirst, outerLast, true);
        long gcUpper = difference(outerFirst, outerLast, false);
        if (cpuLower > cpuUpper || gcLower > gcUpper) { return unknown("RESOURCE_COUNTER_ENCLOSURE_CONTRADICTION"); }
        var out = base("RECORDED_COUNTER_ENCLOSURE", innerReads < 2
                ? "ZERO_LOWER_BOUND_FOLLOWS_MONOTONICITY_WITHOUT_TWO_INNER_READS" : "INNER_AND_OUTER_REPORTED_COUNTER_DELTAS");
        out.put("startBoundsNanos", List.of(startLower, startUpper)); out.put("endBoundsNanos", List.of(endLower, endUpper));
        out.put("cpuNanosLower", cpuLower); out.put("cpuNanosUpper", cpuUpper);
        out.put("gcCollectionMillisLower", gcLower); out.put("gcCollectionMillisUpper", gcUpper);
        out.put("outerFirstRead", bracket(outerFirst)); out.put("outerLastRead", bracket(outerLast));
        out.put("innerReadCount", innerReads); out.put("possibleWindowReadCount", possibleReads);
        if (innerReads > 0) {
            out.put("innerSampledHeapPeakBytes", innerHeap); out.put("innerSampledRssPeakBytes", innerRss);
            out.put("innerFirstRead", bracket(innerFirst)); out.put("innerLastRead", bracket(innerLast));
        }
        if (possibleReads > 0) {
            out.put("possibleWindowSampledHeapPeakBytes", possibleHeap); out.put("possibleWindowSampledRssPeakBytes", possibleRss);
        }
        out.put("memoryScope", "ACTUAL_SAMPLED_VALUES_ONLY_NO_CONTINUOUS_PEAK_BOUND");
        out.put("fullPhaseSampledHeapPeakBytes", full.peakHeapBytes()); out.put("fullPhaseSampledRssPeakBytes", full.peakRssBytes());
        out.put("resourceReadTotalDurationNanos", totalReadNanos); out.put("resourceReadMaximumDurationNanos", maximumReadNanos);
        out.put("resourceReadCount", attempts.size());
        return Map.copyOf(out);
    }

    private static long difference(BenchmarkResourceSampler.Attempt first, BenchmarkResourceSampler.Attempt last, boolean cpu) {
        return cpu ? Math.subtractExact(last.reading().cpuNanos().orElseThrow(), first.reading().cpuNanos().orElseThrow())
                : Math.subtractExact(last.reading().gcCollectionMillis().orElseThrow(), first.reading().gcCollectionMillis().orElseThrow());
    }
    private static Map<String, Object> bracket(BenchmarkResourceSampler.Attempt attempt) {
        return Map.of("index", attempt.index(), "startedAtNanos", attempt.startedAtNanos(),
                "completedAtNanos", attempt.completedAtNanos());
    }
    private static Map<String, Object> unknown(String reason) { return Map.copyOf(base("UNKNOWN", reason)); }
    private static Map<String, Object> base(String state, String reason) {
        var out = new LinkedHashMap<String, Object>();
        out.put("state", state); out.put("reason", reason);
        out.put("ownedIdentityProof", "CALLER_OWNED_RESOURCE_CONNECTION");
        out.put("counterScope", "REPORTED_CUMULATIVE_CPU_AND_GC_COUNTERS_CONDITIONAL_ON_MONOTONICITY");
        out.put("samplingCostQualified", false); out.put("performanceAcceptanceEligible", false);
        return out;
    }
}
