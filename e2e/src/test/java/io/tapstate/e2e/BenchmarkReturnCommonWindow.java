package io.tapstate.e2e;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A conservative common-window certificate, never runtime provenance or formal performance qualification. */
final class BenchmarkReturnCommonWindow {
    private static final int BINS = 10;
    private static final int MAX_ROWS = 512 * 512;
    private static final int MAX_CALLS = 512;
    private static final int MAX_CALL_ROWS = 1024;
    private static final BigInteger TEN = BigInteger.TEN;
    private BenchmarkReturnCommonWindow() { }

    private record TargetKey(String target, String key) { }
    private static final class Call {
        final String target;
        final BenchmarkCausalClock.Interval returned;
        int weight;
        Call(String target, BenchmarkCausalClock.Interval returned) { this.target = target; this.returned = returned; }
    }

    static Map<String, Object> evidence(List<BenchmarkReturnTimeBounds.Delivery> completeCohort) {
        require(completeCohort != null && !completeCohort.isEmpty() && completeCohort.size() <= MAX_ROWS,
                "complete bounded cohort is required");
        var calls = new LinkedHashMap<Long, Call>();
        var targets = new LinkedHashMap<String, List<Call>>();
        var keys = new HashSet<TargetKey>();
        for (var row : completeCohort) {
            require(row != null && row.literalReturn() != null && row.callSequence() > 0, "row or call identity is missing");
            text(row.target()); text(row.key());
            require(keys.add(new TargetKey(row.target(), row.key())), "target key is duplicate");
            Call call = calls.get(row.callSequence());
            if (call == null) {
                require(calls.size() < MAX_CALLS, "call roster exceeds its bound");
                call = new Call(row.target(), row.literalReturn());
                calls.put(row.callSequence(), call);
                targets.computeIfAbsent(row.target(), unused -> new ArrayList<>()).add(call);
                require(targets.size() <= 2, "target roster differs from the fixed workloads");
            } else {
                require(call.target.equals(row.target()) && call.returned.equals(row.literalReturn()),
                        "one full call has inconsistent target or return bounds");
            }
            require(++call.weight <= MAX_CALL_ROWS, "one call exceeds its native row bound");
        }

        long startLower = Long.MIN_VALUE, startUpper = Long.MIN_VALUE;
        long endLower = Long.MAX_VALUE, endUpper = Long.MAX_VALUE;
        var targetWindows = new ArrayList<Map<String, Object>>();
        var targetLastLower = new LinkedHashMap<String, Long>();
        for (var target : targets.entrySet()) {
            long firstLower = Long.MAX_VALUE, firstUpper = Long.MAX_VALUE;
            long lastLower = Long.MIN_VALUE, lastUpper = Long.MIN_VALUE;
            for (Call call : target.getValue()) {
                firstLower = Math.min(firstLower, call.returned.lowerNanos());
                firstUpper = Math.min(firstUpper, call.returned.upperNanos());
                lastLower = Math.max(lastLower, call.returned.lowerNanos());
                lastUpper = Math.max(lastUpper, call.returned.upperNanos());
            }
            startLower = Math.max(startLower, firstLower); startUpper = Math.max(startUpper, firstUpper);
            endLower = Math.min(endLower, lastLower); endUpper = Math.min(endUpper, lastUpper);
            targetLastLower.put(target.getKey(), lastLower);
            targetWindows.add(Map.of("target", target.getKey(), "firstBoundsNanos", List.of(firstLower, firstUpper),
                    "lastBoundsNanos", List.of(lastLower, lastUpper)));
        }
        var out = new LinkedHashMap<String, Object>();
        out.put("performanceAcceptanceEligible", false);
        out.put("samplingCostQualified", false);
        out.put("membershipRule", "ORIGINAL_OPEN_LOWER_CLOSED_UPPER_COMMON_TARGET_INTERVAL");
        out.put("rowTimeAssignment", "ONE_LITERAL_RETURN_VARIABLE_PER_FULL_CALL_WITH_SHARED_ROW_WEIGHT");
        out.put("endpointProof", "CALLER_SUPPLIED_COMPLETE_COHORT_AND_LITERAL_RETURN_BOUNDS");
        out.put("targetWindows", List.copyOf(targetWindows));
        out.put("commonStartBoundsNanos", List.of(startLower, startUpper));
        out.put("commonEndBoundsNanos", List.of(endLower, endUpper));
        out.put("fullCohortRows", completeCohort.size()); out.put("fullCallCount", calls.size());

        long completedLower = 0, completedUpper = 0, earlyLower = 0, earlyUpper = 0, lateLower = 0, lateUpper = 0;
        long[] binLower = new long[BINS], binUpper = new long[BINS];
        BigInteger halfLower = integer(startLower).add(integer(endLower));
        BigInteger halfUpper = integer(startUpper).add(integer(endUpper));
        for (Call call : calls.values()) {
            long lower = call.returned.lowerNanos(), upper = call.returned.upperNanos();
            // A call is always at or before its own target's maximum; only other targets constrain the closed end.
            boolean beforeOtherEnds = targetLastLower.entrySet().stream().allMatch(target ->
                    target.getKey().equals(call.target) || upper <= target.getValue());
            boolean definite = lower > startUpper && beforeOtherEnds;
            boolean possible = upper > startLower && lower <= endUpper;
            boolean ownLatest = targets.get(call.target).stream().allMatch(other ->
                    other == call || lower >= other.returned.upperNanos());
            // Such a call is the common-end variable itself, preserving the inclusive final bin.
            boolean atCommonEnd = ownLatest && beforeOtherEnds;
            if (definite) { completedLower += call.weight; }
            if (possible) { completedUpper += call.weight; }
            if (definite && integer(upper).shiftLeft(1).compareTo(halfLower) < 0) { earlyLower += call.weight; }
            if (possible && integer(lower).shiftLeft(1).compareTo(halfUpper) < 0) { earlyUpper += call.weight; }
            if (definite && integer(lower).shiftLeft(1).compareTo(halfUpper) >= 0) { lateLower += call.weight; }
            if (possible && integer(upper).shiftLeft(1).compareTo(halfLower) >= 0) { lateUpper += call.weight; }
            BigInteger scaledLower = integer(lower).multiply(TEN), scaledUpper = integer(upper).multiply(TEN);
            for (int bin = 0; bin < BINS; bin++) {
                boolean definitelyAbove = bin == 0 || bin == BINS - 1 && atCommonEnd
                        || scaledLower.compareTo(edge(bin, startUpper, endUpper)) >= 0;
                boolean definitelyBelow = bin == BINS - 1 || scaledUpper.compareTo(edge(bin + 1, startLower, endLower)) < 0;
                boolean possiblyAbove = bin == 0 || scaledUpper.compareTo(edge(bin, startLower, endLower)) >= 0;
                boolean possiblyBelow = bin == BINS - 1 || scaledLower.compareTo(edge(bin + 1, startUpper, endUpper)) < 0;
                if (definite && definitelyAbove && definitelyBelow) { binLower[bin] += call.weight; }
                if (possible && possiblyAbove && possiblyBelow) { binUpper[bin] += call.weight; }
            }
        }
        var bins = new ArrayList<Map<String, Object>>(BINS);
        boolean everyBinProgress = true;
        for (int bin = 0; bin < BINS; bin++) {
            bins.add(Map.of("bin", bin, "lowerRows", binLower[bin], "upperRows", binUpper[bin]));
            everyBinProgress &= binLower[bin] > 0;
        }
        out.put("completedRowsLower", completedLower); out.put("completedRowsUpper", completedUpper);
        out.put("ambiguousMembershipRows", completedUpper - completedLower); out.put("bins", List.copyOf(bins));
        out.put("earlyRowsLower", earlyLower); out.put("earlyRowsUpper", earlyUpper);
        out.put("lateRowsLower", lateLower); out.put("lateRowsUpper", lateUpper);
        out.put("halfTrendRule", "20_TIMES_ABSOLUTE_LATE_MINUS_EARLY_AT_MOST_EARLY");
        if (earlyUpper > 0) { out.put("lateToEarlyRatioLower", ratio(lateLower, earlyUpper)); }
        if (earlyLower > 0) {
            out.put("lateToEarlyRatioUpper", ratio(lateUpper, earlyLower));
            long lowDeviation = Math.abs(lateLower - earlyUpper), highDeviation = Math.abs(lateUpper - earlyLower);
            boolean chooseLow = integer(lowDeviation).multiply(integer(earlyLower))
                    .compareTo(integer(highDeviation).multiply(integer(earlyUpper))) >= 0;
            out.put("halfTrendUpper", ratio(chooseLow ? lowDeviation : highDeviation, chooseLow ? earlyUpper : earlyLower));
        } else { out.put("halfTrendUpperUnbounded", true); }

        String reason;
        try {
            long durationLower = Math.subtractExact(endLower, startUpper);
            long durationUpper = Math.subtractExact(endUpper, startLower);
            out.put("durationNanosLower", durationLower); out.put("durationNanosUpper", durationUpper);
            if (durationLower <= 0) { reason = "NO_GUARANTEED_POSITIVE_COMMON_INTERVAL"; }
            else {
                if (completedLower > 0) {
                    var span = new BenchmarkCausalClock.Interval(durationLower, durationUpper);
                    double rateLower = BenchmarkReturnTimeBounds.throughput(completedLower, span).lowerRecordsPerSecond();
                    double rateUpper = BenchmarkReturnTimeBounds.throughput(completedUpper, span).upperRecordsPerSecond();
                    out.put("recordsPerSecondLower", rateLower); out.put("recordsPerSecondUpper", rateUpper);
                }
                if (completedLower < 10_000) { reason = "COMPLETED_MEMBERSHIP_NOT_PROVEN_AT_LEAST_10000"; }
                else if (!everyBinProgress) { reason = "TEN_BIN_PROGRESS_NOT_PROVEN"; }
                else if (20L * lateUpper > 21L * earlyLower || 19L * earlyUpper > 20L * lateLower) {
                    reason = "FIVE_PERCENT_HALF_TREND_NOT_PROVEN";
                } else { reason = "ALL_CONSERVATIVE_COMMON_WINDOW_RULES_HOLD"; }
            }
        } catch (ArithmeticException overflow) { reason = "COMMON_WINDOW_DURATION_OVERFLOW"; }
        boolean passed = reason.equals("ALL_CONSERVATIVE_COMMON_WINDOW_RULES_HOLD");
        out.put("state", passed ? "PASS" : "UNQUALIFIED"); out.put("reason", reason);
        out.put("commonWindowCertificatePassed", passed);
        return Map.copyOf(out);
    }

    private static BigInteger edge(int bin, long start, long end) {
        return integer(start).multiply(integer(BINS - bin)).add(integer(end).multiply(integer(bin)));
    }
    private static BigInteger integer(long value) { return BigInteger.valueOf(value); }
    private static Map<String, Object> ratio(long numerator, long denominator) {
        return Map.of("numerator", numerator, "denominator", denominator);
    }
    private static void text(String value) {
        require(value != null && !value.isBlank() && value.length() <= 512
                && value.getBytes(StandardCharsets.UTF_8).length <= 512, "target or key is missing or exceeds its bound");
    }
    private static void require(boolean condition, String reason) {
        if (!condition) { throw new AssertionError("return common window " + reason); }
    }
}
