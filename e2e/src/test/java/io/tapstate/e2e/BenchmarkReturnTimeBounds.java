package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.List;

/** Conservative return-time arithmetic; none of these bounds substitutes for live method evidence. */
final class BenchmarkReturnTimeBounds {
    private static final int MAX_ROWS = 512 * 512;
    private BenchmarkReturnTimeBounds() { }

    record Delivery(String target, String key, long callSequence,
                    BenchmarkCausalClock.Interval capturedPoint,
                    BenchmarkCausalClock.Interval literalReturn,
                    BenchmarkCausalClock.Interval latency) { }
    record Rate(double lowerRecordsPerSecond, double upperRecordsPerSecond) { }

    /** Each row shares its full call's return bound; this is not a per-row physical commit time. */
    static List<Delivery> map(List<BenchmarkWriteReturnExpectations.Association> associations,
                              BenchmarkCausalClock.Identity owner, BenchmarkReturnPointClock clock) {
        require(associations != null && associations.size() <= MAX_ROWS && owner != null && clock != null,
                "bounded source associations and an owned clock are required");
        BenchmarkReturnWorkBudget.add(BenchmarkReturnWorkBudget.Activity.COPY, BenchmarkReturnWorkBudget.Work.pass(associations.size()));
        List<Delivery> deliveries = new ArrayList<>();
        var workScope = BenchmarkReturnWorkBudget.current();
        for (var row : associations) {
            require(row != null, "source association is missing");
            if (!row.fixedCohort()) { continue; }
            if (workScope != null) {
                BenchmarkReturnWorkBudget.add(workScope, BenchmarkReturnWorkBudget.Activity.RETURN_MAP, new BenchmarkReturnWorkBudget.Work(2, 0, 1, 0, 0, 1, 2, 0));
                BenchmarkReturnWorkBudget.add(workScope, BenchmarkReturnWorkBudget.Activity.SOURCE_MAP, BenchmarkReturnWorkBudget.Work.records(1));
            }
            try {
                require(Math.subtractExact(row.callLastCallbackExitNanos(), row.callBeganNanos()) >= 0
                        && Math.subtractExact(row.callObservedNanos(), row.callLastCallbackExitNanos()) >= 0,
                        "owned call callback clock order is invalid");
            } catch (ArithmeticException overflow) { throw new AssertionError("return time bounds owned call span overflow", overflow); }
            var exited = clock.map(owner, row.callLastCallbackExitNanos());
            var observed = clock.map(owner, row.callObservedNanos());
            var returned = new BenchmarkCausalClock.Interval(exited.lowerNanos(), observed.upperNanos());
            var latency = BenchmarkCausalClock.elapsed(clock.sourcePoint(row.sourceIssuedAtNanos()), returned);
            require(latency.upperNanos() >= 0, "return bound is wholly before its registered source issue");
            deliveries.add(new Delivery(row.target().pipelineId() + "/" + row.target().table(), row.key(),
                    row.callSequence(), observed, returned, latency));
        }
        return List.copyOf(deliveries);
    }

    /** The lower and upper order statistics enclose every admissible p99 without choosing midpoints. */
    static BenchmarkCausalClock.Interval p99(List<BenchmarkCausalClock.Interval> latencies) {
        require(latencies != null && latencies.size() >= 10_000 && latencies.size() <= MAX_ROWS,
                "p99 requires at least 10000 complete deliveries within the record bound");
        List<BenchmarkCausalClock.Interval> values = List.copyOf(latencies);
        int rank = (int) ((99L * values.size() + 99) / 100) - 1;
        BenchmarkReturnWorkBudget.add(BenchmarkReturnWorkBudget.Activity.SORT,
                new BenchmarkReturnWorkBudget.Work(2, 2, 0, 0, 0, 2, Math.multiplyExact(2L, values.size()), 0));
        long lower = values.stream().map(BenchmarkCausalClock.Interval::lowerNanos).sorted().skip(rank).findFirst().orElseThrow();
        long upper = values.stream().map(BenchmarkCausalClock.Interval::upperNanos).sorted().skip(rank).findFirst().orElseThrow();
        return new BenchmarkCausalClock.Interval(lower, upper);
    }

    /** Covers the actual earliest-to-latest completed-call span; ambiguous zero spans stay unqualified. */
    static BenchmarkCausalClock.Interval span(List<BenchmarkCausalClock.Interval> returns) {
        require(returns != null && returns.size() >= 2 && returns.size() <= MAX_ROWS,
                "return span needs a bounded complete roster");
        List<BenchmarkCausalClock.Interval> values = List.copyOf(returns);
        BenchmarkReturnWorkBudget.add(BenchmarkReturnWorkBudget.Activity.SPAN,
                new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0, 0, 4, Math.multiplyExact(4L, values.size()), 0));
        long firstLower = values.stream().mapToLong(BenchmarkCausalClock.Interval::lowerNanos).min().orElseThrow();
        long firstUpper = values.stream().mapToLong(BenchmarkCausalClock.Interval::upperNanos).min().orElseThrow();
        long lastLower = values.stream().mapToLong(BenchmarkCausalClock.Interval::lowerNanos).max().orElseThrow();
        long lastUpper = values.stream().mapToLong(BenchmarkCausalClock.Interval::upperNanos).max().orElseThrow();
        return BenchmarkCausalClock.elapsed(new BenchmarkCausalClock.Interval(firstLower, firstUpper),
                new BenchmarkCausalClock.Interval(lastLower, lastUpper));
    }

    static Rate throughput(long completeRecords, BenchmarkCausalClock.Interval elapsed) {
        require(completeRecords > 0 && completeRecords <= MAX_ROWS && elapsed != null && elapsed.lowerNanos() > 0,
                "throughput needs a complete count and a strictly positive duration lower bound");
        var numerator = java.math.BigDecimal.valueOf(completeRecords).multiply(java.math.BigDecimal.valueOf(1_000_000_000L));
        double lower = Math.nextDown(numerator.divide(java.math.BigDecimal.valueOf(elapsed.upperNanos()),
                new java.math.MathContext(17, java.math.RoundingMode.FLOOR)).doubleValue());
        double upper = Math.nextUp(numerator.divide(java.math.BigDecimal.valueOf(elapsed.lowerNanos()),
                new java.math.MathContext(17, java.math.RoundingMode.CEILING)).doubleValue());
        require(Double.isFinite(lower) && Double.isFinite(upper) && lower > 0 && lower <= upper,
                "throughput bounds are invalid");
        return new Rate(lower, upper);
    }

    private static void require(boolean condition, String reason) {
        if (!condition) { throw new AssertionError("return time bounds " + reason); }
    }
}
