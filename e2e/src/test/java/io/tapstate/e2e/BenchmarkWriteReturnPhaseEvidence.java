package io.tapstate.e2e;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Complete diagnostic source and return receipts, with conservative arithmetic and no acceptance credit. */
final class BenchmarkWriteReturnPhaseEvidence {
    private static final int MAX_PAGE_BYTES = 64 * 1024;
    private static final int MAX_LEDGER_BYTES = 2 * 1024 * 1024;
    private static final int MAX_PAGES = 512;

    private BenchmarkWriteReturnPhaseEvidence() { }

    static Map<String, Object> record(BenchmarkWorkloadDefinitions.Workload workload,
            BenchmarkWorkloadDefinitions.Phase phase, List<BenchmarkForkEnvironment.BatchResult> sourceBatches,
            BenchmarkWriteReturnCapture.Result capture) {
        return record(workload, phase, sourceBatches, capture, null);
    }

    static Map<String, Object> record(BenchmarkWorkloadDefinitions.Workload workload,
            BenchmarkWorkloadDefinitions.Phase phase, List<BenchmarkForkEnvironment.BatchResult> sourceBatches,
            BenchmarkWriteReturnCapture.Result capture, BenchmarkNativeReturnClock nativeClock) {
        require(workload != null && phase != null && sourceBatches != null
                && capture != null && capture.summary() != null, "actual capture or source registration is missing");
        var summary = capture.summary();
        require((workload.id() + "/" + phase.id()).equals(summary.window()), "capture belongs to another workload or phase");
        require("RECORDED_SCOPE_UNQUALIFIED".equals(summary.state()) && summary.failedCalls() == 0
                && summary.openCalls() == 0 && summary.completedCalls() > 0, "capture terminal counters are unqualified");
        Raw raw = verifyRaw(capture);
        var rows = BenchmarkWriteReturnExpectations.associate(workload, phase, sourceBatches, capture.calls());
        require(rows.size() == phase.expectedLogicalOutputChanges() && summary.reportedRecords() == rows.size(),
                "complete source and original row totals differ");
        long expectedCohort = phase.expectedLogicalOutputChanges();
        if (workload.pilotProfile()) {
            require(expectedCohort % 2 == 0, "pilot cohort registration is not an exact half");
            expectedCohort /= 2;
        }
        long cohort = rows.stream().filter(BenchmarkWriteReturnExpectations.Association::fixedCohort).count();
        require(cohort == expectedCohort, "fixed cohort is incomplete");
        require(!capture.samples().isEmpty(), "actual owned clock samples are missing");
        var owner = capture.samples().getFirst().identity();
        BenchmarkReturnPointClock clock = nativeClock == null ? new BenchmarkCausalClock(owner, capture.samples()) : nativeClock;
        require(nativeClock == null || capture.clockMode() == BenchmarkReturnClockSampler.Mode.PERIODIC,
                "common native counter diagnostics require the complete periodic sample roster");
        for (var call : capture.calls()) {
            clock.map(owner, call.beganNanos());
            clock.map(owner, call.lastCallbackExitNanos());
            clock.map(owner, call.observedNanos());
        }
        boolean clockControl = capture.clockMode() == BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL;
        Map<String, Object> p99 = unavailable("FIRST_FINAL_CLOCK_COST_CONTROL");
        Map<String, Object> span = p99;
        Map<String, Object> throughput = p99;
        if (clockControl) {
            require(capture.samples().size() == 2, "first/final control must retain exactly two actual samples");
        } else {
            var deliveries = BenchmarkReturnTimeBounds.map(rows, owner, clock);
            require(deliveries.size() == cohort, "return bounds do not cover the complete fixed cohort");
            p99 = deliveries.size() < 10_000
                    ? unavailable("INSUFFICIENT_COMPLETE_DELIVERIES_FOR_P99")
                    : interval(BenchmarkReturnTimeBounds.p99(deliveries.stream()
                            .map(BenchmarkReturnTimeBounds.Delivery::latency).toList()));
            span = unavailable("INSUFFICIENT_COMPLETE_DELIVERIES_FOR_SPAN");
            throughput = unavailable("INSUFFICIENT_COMPLETE_DELIVERIES_FOR_SPAN");
            if (deliveries.size() >= 2) {
                var bounds = BenchmarkReturnTimeBounds.span(deliveries.stream()
                        .map(BenchmarkReturnTimeBounds.Delivery::literalReturn).toList());
                span = interval(bounds);
                if (bounds.lowerNanos() > 0) {
                    var rate = BenchmarkReturnTimeBounds.throughput(cohort, bounds);
                    throughput = Map.of("state", "RECORDED_INTERVAL", "lowerRecordsPerSecond", rate.lowerRecordsPerSecond(),
                            "upperRecordsPerSecond", rate.upperRecordsPerSecond());
                } else {
                    throughput = unavailable("NONPOSITIVE_COMPLETION_SPAN_LOWER_BOUND");
                }
            }
        }
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("state", clockControl ? "RECORDED_CLOCK_COST_CONTROL" : nativeClock == null
                ? "RECORDED_CAUSAL_RETURN_BOUNDS" : "CONDITIONAL_NATIVE_COUNTER_RETURN_BOUNDS");
        if (nativeClock != null) { evidence.put("nativeCounterDomainMapping", nativeClock.evidence()); }
        evidence.put("clockSamplingMode", capture.clockMode().name());
        evidence.put("workload", workload.id()); evidence.put("phase", phase.id());
        evidence.put("endpoint", "ordinary nontransactional acknowledged writeRecord successful return");
        evidence.put("timestampMeaning", "first owned nanoTime observation after a full table call returns");
        evidence.put("rowTimeAssignment", "rows share their full call's conservative return interval");
        evidence.put("literalReturnLowerBound", "last synchronous callback exit in the same owned writer thread");
        evidence.put("physicalPerRowCommitTime", false);
        evidence.put("ownedRuntime", Map.of("pid", owner.pid(), "jvmStartTimeMillis", owner.jvmStartTimeMillis()));
        evidence.put("sourceBatches", sourceBatches.stream().map(batch -> Map.<String, Object>of(
                "index", batch.index(), "issuedAtNanos", batch.issuedAtNanos(), "completedAtNanos", batch.completedAtNanos())).toList());
        evidence.put("clockSamples", capture.samples().stream().map(sample -> Map.<String, Object>of(
                "sequence", sample.sequence(), "pid", sample.identity().pid(),
                "jvmStartTimeMillis", sample.identity().jvmStartTimeMillis(), "driverBeforeNanos", sample.driverBeforeNanos(),
                "driverAfterNanos", sample.driverAfterNanos(), "ownedNanos", sample.ownedNanos())).toList());
        evidence.put("summary", Map.of("window", summary.window(), "state", summary.state(),
                "completedCalls", summary.completedCalls(), "failedCalls", summary.failedCalls(),
                "reportedRecords", summary.reportedRecords(), "openCalls", summary.openCalls(),
                "retainedBytes", summary.retainedBytes()));
        evidence.put("pagesBase64", List.copyOf(capture.pagesBase64()));
        evidence.put("captureEpoch", raw.epoch()); evidence.put("frameCount", raw.frames());
        evidence.put("rawPageBytes", raw.bytes()); evidence.put("pageCount", capture.pagesBase64().size());
        evidence.put("callCount", capture.calls().size()); evidence.put("fullRows", rows.size());
        evidence.put("fixedCohortRows", cohort); evidence.put("p99LatencyNanos", p99);
        evidence.put("completionSpanNanos", span); evidence.put("throughput", throughput);
        evidence.put("completionSpanScope", "FULL_FIXED_COHORT_EARLIEST_TO_LATEST_RETURN");
        evidence.put("formalCommonActiveWindowQualified", false);
        evidence.put("performanceAcceptanceEligible", false);
        evidence.put("returnCaptureDelayQualified", false); evidence.put("samplingCostQualified", false);
        if (!capture.costStages().isEmpty()) { evidence.put("producerCostStages", capture.costStages()); }
        return Map.copyOf(evidence);
    }

    private record Raw(long epoch, int frames, int bytes) { }

    /** The raw, original pages must reproduce every supplied call and the actual terminal totals. */
    private static Raw verifyRaw(BenchmarkWriteReturnCapture.Result capture) {
        var pages = capture.pagesBase64();
        require(pages != null && !pages.isEmpty() && pages.size() <= MAX_PAGES, "raw page roster is missing or exceeds its bound");
        var summary = capture.summary();
        var assembly = new BenchmarkWriteReturnAssembly(summary.window());
        int bytes = 0, frames = 0;
        long epoch = 0, stored = 0;
        for (String encoded : pages) {
            require(encoded != null && encoded.length() <= 4 * ((MAX_PAGE_BYTES + 2) / 3), "encoded raw page exceeds its bound");
            byte[] pageBytes;
            try { pageBytes = Base64.getDecoder().decode(encoded); }
            catch (IllegalArgumentException invalid) { throw new AssertionError("return phase evidence raw page is not base64", invalid); }
            require(pageBytes.length <= MAX_PAGE_BYTES && Base64.getEncoder().encodeToString(pageBytes).equals(encoded),
                    "raw page is oversized or noncanonical");
            bytes = Math.addExact(bytes, pageBytes.length);
            require(bytes <= MAX_LEDGER_BYTES, "raw ledger exceeds its byte bound");
            var page = BenchmarkWriteReturnLedger.decode(pageBytes);
            if (epoch == 0) epoch = page.epoch();
            require(epoch > 0 && page.epoch() == epoch, "raw capture epoch changed");
            int header = 32 + page.window().getBytes(StandardCharsets.UTF_8).length
                    + page.state().getBytes(StandardCharsets.UTF_8).length;
            stored = Math.addExact(stored, pageBytes.length - header);
            frames = page.totalFrames();
            assembly.add(pageBytes);
        }
        require(stored == summary.retainedBytes(), "raw and terminal retained-byte totals differ");
        var decoded = assembly.finish(summary.completedCalls(), summary.reportedRecords());
        require(decoded.equals(capture.calls()), "raw pages do not reproduce the supplied complete calls");
        return new Raw(epoch, frames, bytes);
    }

    private static Map<String, Object> interval(BenchmarkCausalClock.Interval bounds) {
        return Map.of("state", "RECORDED_INTERVAL", "lowerNanos", bounds.lowerNanos(), "upperNanos", bounds.upperNanos());
    }
    private static Map<String, Object> unavailable(String reason) {
        return Map.of("state", "UNAVAILABLE", "reason", reason);
    }
    private static void require(boolean condition, String reason) {
        if (!condition) throw new AssertionError("return phase evidence " + reason);
    }
}
