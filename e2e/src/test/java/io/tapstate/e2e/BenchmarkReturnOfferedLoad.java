package io.tapstate.e2e;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Fixed-offer qualification of one measured phase: across the fixed cohort's source batches the source held
 * its declared interval, and the cohort's common return window spans the same time as their issuance, so
 * completions tracked a constant offered load instead of draining or growing a backlog. Both spans are
 * monotonic nanosecond spans on one host; the check is a ratio and claims no SI or UTC accuracy.
 */
final class BenchmarkReturnOfferedLoad {
    static final long TOLERANCE_PERCENT = 5;

    private BenchmarkReturnOfferedLoad() { }

    static Map<String, Object> evidence(BenchmarkWorkloadDefinitions.Workload workload, BenchmarkWorkloadDefinitions.Phase phase,
            List<BenchmarkForkEnvironment.BatchResult> batches, long durationLower, long durationUpper) {
        require(workload != null && phase != null && phase.measured() && batches != null && !batches.isEmpty(),
                "measured phase input is missing");
        require(durationLower > 0 && durationUpper >= durationLower, "common return window duration is not positive");
        for (int index = 0; index < batches.size(); index++) {
            require(batches.get(index) != null && batches.get(index).index() == index, "source batch ledger is incomplete or out of order");
        }
        var cohortBatches = new TreeSet<Integer>();
        for (var plan : BenchmarkExpectedChanges.forPhase(workload, phase)) {
            require(plan.batches().size() == batches.size(), "target and source batch rosters differ");
            for (int index = 0; index < plan.batches().size(); index++) {
                for (var change : plan.forBatch(index)) {
                    if (workload.inFixedCohort(change.key())) { cohortBatches.add(index); }
                }
            }
        }
        require(cohortBatches.size() >= 2, "the fixed cohort spans fewer than two source batches");
        long interval = phase.batchInterval().toNanos();
        require(interval > 0, "the measured phase declares no fixed offer interval");
        int first = cohortBatches.first(), last = cohortBatches.last();
        long nominalSpan = Math.multiplyExact((long) last - first, interval);
        long issueSpan = Math.subtractExact(batches.get(last).issuedAtNanos(), batches.get(first).issuedAtNanos());
        boolean paceHeld = within(issueSpan, issueSpan, nominalSpan);
        boolean tracked = within(durationLower, durationUpper, issueSpan);
        var out = new LinkedHashMap<String, Object>();
        out.put("rule", "COHORT_ISSUE_SPAN_AND_COMMON_RETURN_SPAN_WITHIN_5_PERCENT");
        out.put("intervalNanos", interval);
        out.put("firstCohortBatch", first); out.put("lastCohortBatch", last); out.put("cohortBatches", cohortBatches.size());
        out.put("nominalIssueSpanNanos", nominalSpan); out.put("actualIssueSpanNanos", issueSpan);
        out.put("commonReturnSpanNanos", List.of(durationLower, durationUpper));
        out.put("sourcePaceHeld", paceHeld); out.put("completionsTrackedIssuance", tracked);
        out.put("state", paceHeld && tracked ? "PASS" : "FAIL");
        out.put("reason", !paceHeld ? "SOURCE_DID_NOT_HOLD_THE_FIXED_OFFER"
                : !tracked ? "COMPLETIONS_DID_NOT_TRACK_THE_FIXED_OFFER" : "FIXED_OFFER_SUSTAINED");
        return Map.copyOf(out);
    }

    /** Whether [lower, upper] lies within the tolerance band around the reference span. */
    private static boolean within(long lower, long upper, long reference) {
        return reference > 0
                && Math.multiplyExact(lower, 100) >= Math.multiplyExact(reference, 100 - TOLERANCE_PERCENT)
                && Math.multiplyExact(upper, 100) <= Math.multiplyExact(reference, 100 + TOLERANCE_PERCENT);
    }

    private static void require(boolean condition, String reason) {
        if (!condition) { throw new AssertionError("return offered load " + reason); }
    }
}
