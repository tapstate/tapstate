package io.tapstate.e2e;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/** Owns one diagnostic capture window; borrowed runtime connections and the child stay caller-owned. */
final class BenchmarkWriteReturnCapture implements AutoCloseable {
    interface Access {
        BenchmarkCausalClock.Sample clock(long sequence);
        boolean start(String window);
        boolean stop();
        BenchmarkWriteReturnReader.Summary summary();
        byte[] page(long cursor);
    }

    record Result(List<BenchmarkWriteReturnAssembly.FullCall> calls,
                  List<BenchmarkCausalClock.Sample> samples, BenchmarkWriteReturnReader.Summary summary,
                  List<String> pagesBase64) {
        Result {
            calls = List.copyOf(calls); samples = List.copyOf(samples); pagesBase64 = List.copyOf(pagesBase64);
        }
    }

    private final Access access;
    private final String window;
    private final BenchmarkReturnClockSampler sampler;
    private boolean captureStarted;
    private boolean completed;
    private boolean closed;
    private long epoch;
    private BenchmarkWriteReturnReader.Summary terminalSummary;
    private final List<String> retainedPages = new ArrayList<>();
    private long retainedPageBytes;

    static BenchmarkWriteReturnCapture open(BenchmarkWriteReturnReader reader, String window) {
        return open(new Access() {
            public BenchmarkCausalClock.Sample clock(long sequence) { return reader.clockSample(sequence); }
            public boolean start(String value) { return reader.start(value); }
            public boolean stop() { return reader.stop(); }
            public BenchmarkWriteReturnReader.Summary summary() { return reader.summary(); }
            public byte[] page(long cursor) { return reader.page(cursor); }
        }, window);
    }

    static BenchmarkWriteReturnCapture open(Access access, String window) {
        var capture = new BenchmarkWriteReturnCapture(access, window);
        try {
            capture.sampler.start();
            if (!access.start(window)) { throw new AssertionError("owned return capture start was refused"); }
            capture.captureStarted = true;
            var initial = BenchmarkWriteReturnLedger.decode(access.page(0));
            if (!window.equals(initial.window()) || initial.cursor() != 0 || initial.epoch() <= 0) {
                throw new AssertionError("owned return capture start has no matching epoch");
            }
            capture.epoch = initial.epoch();
            return capture;
        } catch (RuntimeException | Error failure) {
            try { capture.close(); } catch (RuntimeException | Error closing) { failure.addSuppressed(closing); }
            throw failure;
        }
    }

    private BenchmarkWriteReturnCapture(Access access, String window) {
        this.access = java.util.Objects.requireNonNull(access);
        if (window == null || window.isBlank() || window.length() > 512) {
            throw new AssertionError("owned return capture window is invalid");
        }
        this.window = window;
        sampler = new BenchmarkReturnClockSampler(access::clock);
    }

    Result finish() {
        if (!captureStarted || closed || completed) { throw new AssertionError("owned return capture is not active"); }
        captureStarted = false;
        boolean stopped = access.stop();
        if (!stopped) { throw new AssertionError("owned return capture stop left an incomplete call"); }
        var samples = sampler.finishAfterSuccessfulStop();
        var summary = access.summary();
        terminalSummary = summary;
        if (!window.equals(summary.window()) || summary.openCalls() != 0) {
            throw new AssertionError("owned return capture terminal window or open-call counters are unqualified");
        }
        var assembly = new BenchmarkWriteReturnAssembly(window);
        int cursor = 0;
        long storedBytes = 0;
        while (true) {
            byte[] bytes = access.page(cursor);
            if (bytes == null || bytes.length > 64 * 1024 || retainedPages.size() >= 512
                    || retainedPageBytes + bytes.length > 2 * 1024 * 1024) {
                throw new AssertionError("owned return capture page exceeds its retention bound");
            }
            retainedPages.add(java.util.Base64.getEncoder().encodeToString(bytes));
            retainedPageBytes += bytes.length;
            var page = BenchmarkWriteReturnLedger.decode(bytes);
            if (page.epoch() != epoch) { throw new AssertionError("owned return capture epoch changed after start"); }
            int headerBytes = 32 + page.window().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                    + page.state().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            storedBytes = Math.addExact(storedBytes, bytes.length - headerBytes);
            assembly.add(bytes);
            cursor = page.nextCursor();
            if (cursor == page.totalFrames()) { break; }
        }
        if (storedBytes != summary.retainedBytes()) {
            throw new AssertionError("owned return capture retained-byte total contradicts its pages");
        }
        if (!"RECORDED_SCOPE_UNQUALIFIED".equals(summary.state()) || summary.failedCalls() != 0) {
            throw new AssertionError("owned return capture terminal scope or counters are unqualified");
        }
        var calls = assembly.finish(summary.completedCalls(), summary.reportedRecords());
        var clock = new BenchmarkCausalClock(samples.getFirst().identity(), samples);
        for (var call : calls) {
            clock.map(samples.getFirst().identity(), call.beganNanos());
            clock.map(samples.getFirst().identity(), call.observedNanos());
        }
        completed = true;
        return new Result(calls, samples, summary, retainedPages);
    }

    /** Keeps already obtained facts after refusal without retrying a remote read or stop. */
    Map<String, Object> retainedEvidence() {
        return Map.of("window", window, "epoch", epoch, "completed", completed,
                "sampler", sampler.evidence(), "samples", sampler.readings().stream().map(sample -> Map.of(
                        "sequence", sample.sequence(), "pid", sample.identity().pid(),
                        "jvmStartTimeMillis", sample.identity().jvmStartTimeMillis(),
                        "driverBeforeNanos", sample.driverBeforeNanos(), "driverAfterNanos", sample.driverAfterNanos(),
                        "ownedNanos", sample.ownedNanos())).toList(),
                "terminalSummaryAvailable", terminalSummary != null,
                "terminalSummary", terminalSummary == null ? Map.of() : Map.of(
                        "window", terminalSummary.window(), "state", terminalSummary.state(),
                        "completedCalls", terminalSummary.completedCalls(), "openCalls", terminalSummary.openCalls(),
                        "reportedRecords", terminalSummary.reportedRecords(), "failedCalls", terminalSummary.failedCalls(),
                        "retainedBytes", terminalSummary.retainedBytes()),
                "retainedPagesBase64", List.copyOf(retainedPages), "performanceAcceptanceEligible", false);
    }

    @Override public void close() {
        if (closed) { return; }
        closed = true;
        Throwable primary = null;
        try {
            if (captureStarted) {
                captureStarted = false;
                if (!access.stop()) { throw new AssertionError("owned return capture abort has an open call"); }
            }
        } catch (RuntimeException | Error failure) { primary = failure; throw failure; }
        finally {
            try { sampler.close(); }
            catch (RuntimeException | Error failure) {
                if (primary != null) { primary.addSuppressed(failure); } else { throw failure; }
            }
        }
    }
}
