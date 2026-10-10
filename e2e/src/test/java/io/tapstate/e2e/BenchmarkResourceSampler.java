package io.tapstate.e2e;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Samples one child JVM throughout a measured window, preserving unavailable readings as failure. */
final class BenchmarkResourceSampler implements AutoCloseable {

    static final int MAX_RETAINED_ATTEMPTS = 256;

    private final BenchmarkProcessProbe probe;
    private final Supplier<BenchmarkProcessProbe.Snapshot> source;
    private final Duration interval;
    private final LongSupplier nanoTime;
    private final TerminationWaiter terminationWaiter;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("benchmark-resource-sampler").factory());
    private final ResourceTotals resources = new ResourceTotals();
    private final AttemptLog attempts;
    private final Object lifecycle = new Object();
    private volatile PublishedDiagnostics published;
    private Throwable failure;
    private volatile Throwable finalSampleFailure;
    private ScheduledFuture<?> periodicSamples;
    private boolean started;
    private boolean finished;
    private BenchmarkCompilationDiagnostics compilationDiagnostics;
    private volatile Throwable compilationInvariantFailure;
    private BenchmarkThreadPointDiagnostics threadPointDiagnostics;
    private volatile Throwable threadPointInvariantFailure;

    private BenchmarkResourceSampler(BenchmarkProcessProbe probe,
                                     Supplier<BenchmarkProcessProbe.Snapshot> source, Duration interval,
                                     LongSupplier nanoTime, TerminationWaiter terminationWaiter) {
        this(probe, source, interval, nanoTime, terminationWaiter, MAX_RETAINED_ATTEMPTS);
    }

    private BenchmarkResourceSampler(BenchmarkProcessProbe probe,
                                     Supplier<BenchmarkProcessProbe.Snapshot> source, Duration interval,
                                     LongSupplier nanoTime, TerminationWaiter terminationWaiter, int retention) {
        attempts = new AttemptLog(retention);
        published = new PublishedDiagnostics(attempts.snapshot(), Optional.empty());
        this.probe = probe;
        this.source = Objects.requireNonNull(source, "sample source");
        this.interval = Objects.requireNonNull(interval, "sample interval");
        this.nanoTime = Objects.requireNonNull(nanoTime, "sample clock");
        this.terminationWaiter = Objects.requireNonNull(terminationWaiter, "termination waiter");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("resource sample interval must be positive");
        }
    }

    static BenchmarkResourceSampler open(long childPid, Duration interval) {
        BenchmarkProcessProbe probe = BenchmarkProcessProbe.open(childPid);
        return new BenchmarkResourceSampler(probe, probe::sample, interval, System::nanoTime,
                ScheduledExecutorService::awaitTermination);
    }

    Map<String, Object> runtimeEvidence() {
        synchronized (lifecycle) {
            if (started || finished) { throw new IllegalStateException("runtime identity must precede resource measurement"); }
            return probe == null ? Map.of("status", "UNKNOWN", "reason", "NO_OWNED_RESOURCE_PROBE")
                    : probe.runtimeEvidence();
        }
    }

    BenchmarkWriteReturnReader writeReturnReader() {
        if (probe == null) { throw new AssertionError("return capture requires the actual owned resource connection"); }
        return probe.writeReturnReader();
    }

    static BenchmarkResourceSampler openForPhaseBudget(long childPid, Duration interval, Duration budget) {
        int retained = Math.toIntExact(budget.toNanos() / interval.toNanos() + 4);
        if (retained < 2 || retained > 4096) { throw new AssertionError("phase resource retention exceeds its declared finite budget"); }
        BenchmarkProcessProbe probe = BenchmarkProcessProbe.open(childPid);
        return new BenchmarkResourceSampler(probe, probe::sample, interval, System::nanoTime,
                ScheduledExecutorService::awaitTermination, retained);
    }

    static BenchmarkResourceSampler from(Supplier<BenchmarkProcessProbe.Snapshot> source, Duration interval) {
        return from(source, interval, System::nanoTime);
    }

    static BenchmarkResourceSampler from(Supplier<BenchmarkProcessProbe.Snapshot> source, Duration interval,
                                         LongSupplier nanoTime) {
        return from(source, interval, nanoTime, ScheduledExecutorService::awaitTermination);
    }

    static BenchmarkResourceSampler from(Supplier<BenchmarkProcessProbe.Snapshot> source, Duration interval,
                                         LongSupplier nanoTime, TerminationWaiter terminationWaiter) {
        return new BenchmarkResourceSampler(null, source, interval, nanoTime, terminationWaiter);
    }

    static BenchmarkResourceSampler fromWithCompilation(Supplier<BenchmarkProcessProbe.Snapshot> source,
            Duration interval, LongSupplier nanoTime, BenchmarkCompilationDiagnostics diagnostics) {
        BenchmarkResourceSampler sampler = from(source, interval, nanoTime);
        sampler.compilationDiagnostics = Objects.requireNonNull(diagnostics, "compilation diagnostics");
        return sampler;
    }

    static BenchmarkResourceSampler fromWithThreadPoints(Supplier<BenchmarkProcessProbe.Snapshot> source,
            Duration interval, LongSupplier nanoTime, BenchmarkThreadPointDiagnostics diagnostics) {
        BenchmarkResourceSampler sampler = from(source, interval, nanoTime);
        sampler.threadPointDiagnostics = Objects.requireNonNull(diagnostics, "thread point diagnostics");
        return sampler;
    }

    @FunctionalInterface
    interface TerminationWaiter {
        boolean await(ScheduledExecutorService worker, long timeout, TimeUnit unit) throws InterruptedException;
    }

    void start() {
        synchronized (lifecycle) {
            if (started || finished) {
                throw new IllegalStateException("resource sampler can start only once");
            }
            started = true;
        }
        record();
        synchronized (lifecycle) {
            if (!finished) {
                periodicSamples = worker.scheduleWithFixedDelay(this::record, interval.toNanos(), interval.toNanos(),
                        TimeUnit.NANOSECONDS);
            }
        }
    }

    void enableCompilationDiagnostics() {
        synchronized (lifecycle) {
            if (started || finished || compilationDiagnostics != null || threadPointDiagnostics != null || probe == null) {
                throw new IllegalStateException("compilation diagnostics require a fresh owned process sampler");
            }
            compilationDiagnostics = BenchmarkCompilationDiagnostics.from(
                    probe::compilationRead, nanoTime, attempts.retention);
        }
    }

    Optional<Map<String, Object>> compilationEvidence() {
        return compilationDiagnostics == null ? Optional.empty()
                : Optional.of(compilationDiagnostics.wireEvidence());
    }

    void enableThreadPointDiagnostics() {
        synchronized (lifecycle) {
            if (started || finished || threadPointDiagnostics != null || compilationDiagnostics != null || probe == null) {
                throw new IllegalStateException("thread point diagnostics require a fresh owned process sampler");
            }
            threadPointDiagnostics = BenchmarkThreadPointDiagnostics.prepare(probe.threadPointReader(), nanoTime);
        }
    }

    Optional<Map<String, Object>> threadPointEvidence() {
        return threadPointDiagnostics == null ? Optional.empty()
                : Optional.of(threadPointDiagnostics.wireEvidence());
    }

    Summary finish() {
        synchronized (lifecycle) {
            if (!started || finished) {
                throw finishFailure(FailureStage.STATE, FailureReason.NO_OPEN_WINDOW,
                        new IllegalStateException("resource sampler has no open measured window"));
            }
            finished = true;
        }
        if (compilationInvariantFailure != null) {
            stopWorker(compilationInvariantFailure);
            throwCompilationInvariant();
        }
        if (threadPointInvariantFailure != null) {
            stopWorker(threadPointInvariantFailure);
            throwThreadPointInvariant();
        }
        try {
            if (periodicSamples != null) {
                periodicSamples.cancel(false);
            }
            // The final read now follows any active periodic read on this worker. The same stop budget
            // bounds it too; the calling thread must never perform an unbounded final source read.
            worker.execute(() -> {
                try {
                    record(true);
                } catch (Throwable finalFailure) {
                    finalSampleFailure = finalFailure;
                }
            });
            worker.shutdown();
            if (!terminationWaiter.await(worker, 5, TimeUnit.SECONDS)) {
                AssertionError timeout = new AssertionError("resource sampler did not stop within five seconds");
                stopWorker(timeout);
                throw finishFailure(FailureStage.SHUTDOWN, FailureReason.SHUTDOWN_TIMEOUT, timeout);
            }
        } catch (SamplingFailure alreadyDiagnosed) {
            throwThreadPointInvariant();
            throw alreadyDiagnosed;
        } catch (InterruptedException interrupted) {
            stopWorker(interrupted);
            Thread.currentThread().interrupt();
            throwThreadPointInvariant();
            throw finishFailure(FailureStage.SHUTDOWN, FailureReason.SHUTDOWN_INTERRUPTED, interrupted);
        } catch (Throwable shutdownFailure) {
            stopWorker(shutdownFailure);
            throwThreadPointInvariant();
            throw finishFailure(FailureStage.SHUTDOWN, FailureReason.SHUTDOWN_ERROR, shutdownFailure);
        }
        throwCompilationInvariant();
        throwThreadPointInvariant();
        try {
            if (finalSampleFailure != null) {
                throw finishFailure(FailureStage.FINAL_SAMPLE, FailureReason.FINAL_SAMPLE_ERROR, finalSampleFailure);
            }
            if (failure != null) {
                SamplingDiagnostics diagnostics = published.completed();
                Outcome outcome = diagnostics.retainedAttempts().getLast().outcome();
                throw finishFailure(FailureStage.READ, outcome == Outcome.UNAVAILABLE
                        ? FailureReason.READ_UNAVAILABLE : FailureReason.READ_ERROR, failure);
            }
        } catch (SamplingFailure alreadyDiagnosed) {
            throw alreadyDiagnosed;
        } catch (Throwable finalSampleFailure) {
            throw finishFailure(FailureStage.FINAL_SAMPLE, FailureReason.FINAL_SAMPLE_ERROR, finalSampleFailure);
        }
        try {
            return resources.summary(Optional.of(published.completed()));
        } catch (Throwable invalidSummary) {
            throw finishFailure(FailureStage.SUMMARY, FailureReason.INVALID_SUMMARY, invalidSummary);
        }
    }

    private void throwCompilationInvariant() {
        // These are programmer errors from the optional diagnostic, never resource-read failures.
        Throwable invariant = compilationInvariantFailure;
        if (invariant instanceof Error error) { throw error; }
        if (invariant instanceof RuntimeException failure) { throw failure; }
    }

    private void throwThreadPointInvariant() {
        Throwable invariant = threadPointInvariantFailure;
        if (invariant instanceof Error error) { throw error; }
        if (invariant instanceof RuntimeException failure) { throw failure; }
    }

    private SamplingFailure finishFailure(FailureStage stage, FailureReason reason, Throwable cause) {
        // A timed-out read may still hold this object's monitor. Only immutable published data is read here.
        PublishedDiagnostics snapshot = published;
        return new SamplingFailure(null, stage, reason, snapshot.completed(), snapshot.pending(), cause);
    }

    private void stopWorker(Throwable cause) {
        try {
            worker.shutdownNow();
        } catch (Throwable stopFailure) {
            cause.addSuppressed(stopFailure);
        }
    }

    private void record() {
        record(false);
    }

    private synchronized void record(boolean finalRead) {
        if (failure != null) {
            return;
        }
        long startedAt = nanoTime.getAsLong();
        published = new PublishedDiagnostics(published.completed(),
                Optional.of(new PendingRead(published.completed().attemptCount() + 1, startedAt, finalRead)));
        BenchmarkProcessProbe.Snapshot sample = null;
        Outcome outcome = Outcome.SUCCESS;
        Throwable unavailable = null;
        String failureType = null;
        try {
            sample = source.get();
            if (sample == null || !sample.complete()) {
                outcome = Outcome.UNAVAILABLE;
                failureType = "INCOMPLETE_SAMPLE";
                unavailable = new AssertionError("external JVM resource sample is incomplete: " + sample);
            }
        } catch (Throwable error) {
            outcome = Outcome.ERROR;
            failureType = error.getClass().getName();
            unavailable = error;
        } finally {
            // This duration covers the external read itself; bookkeeping and scheduled delay are separate.
            attempts.append(startedAt, nanoTime.getAsLong(), outcome, failureType, sample);
            published = new PublishedDiagnostics(attempts.snapshot(), Optional.empty());
        }
        if (unavailable != null) {
            failure = unavailable;
        } else {
            resources.add(sample);
        }
        if (compilationDiagnostics != null && unavailable == null && compilationInvariantFailure == null) {
            try {
                compilationDiagnostics.record();
            } catch (RuntimeException | Error invariant) {
                compilationInvariantFailure = invariant;
                throw invariant;
            }
        }
        if (threadPointDiagnostics != null && unavailable == null && threadPointInvariantFailure == null) {
            try {
                threadPointDiagnostics.recordAttempt(published.completed().attemptCount());
            } catch (RuntimeException | Error invariant) {
                threadPointInvariantFailure = invariant;
                throw invariant;
            }
        }
    }

    static Summary summarize(List<BenchmarkProcessProbe.Snapshot> readings) {
        if (readings == null || readings.size() < 2) {
            throw new AssertionError("resource window needs at least a start and end sample");
        }
        ResourceTotals totals = new ResourceTotals();
        for (BenchmarkProcessProbe.Snapshot reading : readings) {
            totals.add(reading);
        }
        return totals.summary(Optional.empty());
    }

    static Summary slice(Summary full, long fromNanos, long toNanos) {
        SamplingDiagnostics trace = full.sampling().orElseThrow(() -> new AssertionError("resource slice has no actual sampling trace"));
        if (toNanos <= fromNanos || trace.omittedAttempts() != 0 || trace.failureCount() != 0) {
            throw new AssertionError("resource slice requires a complete bounded successful trace");
        }
        var kept = trace.retainedAttempts().stream().filter(attempt -> attempt.startedAtNanos() >= fromNanos
                && attempt.completedAtNanos() <= toNanos).toList();
        var totals = new ResourceTotals();
        for (var attempt : kept) { totals.add(attempt.reading()); }
        return totals.summary(Optional.of(new SamplingDiagnostics("COHORT_INTERVAL", kept.size(), 0,
                kept.stream().mapToLong(Attempt::durationNanos).sum(),
                kept.stream().mapToLong(Attempt::durationNanos).max().orElse(0), 0, kept,
                trace.retainedAttemptLimit())));
    }

    record Summary(long cpuNanos, long gcCollectionMillis, long peakHeapBytes, long peakRssBytes, int sampleCount,
                   Optional<SamplingDiagnostics> sampling) {
        Summary {
            Objects.requireNonNull(sampling, "sampling diagnostics");
        }

        Summary(long cpuNanos, long gcCollectionMillis, long peakHeapBytes, long peakRssBytes, int sampleCount) {
            this(cpuNanos, gcCollectionMillis, peakHeapBytes, peakRssBytes, sampleCount, Optional.empty());
        }
    }

    enum Outcome {
        SUCCESS,
        UNAVAILABLE,
        ERROR
    }

    record Attempt(long index, long startedAtNanos, long completedAtNanos, Outcome outcome,
                   String failureType, BenchmarkProcessProbe.Snapshot reading) {
        Attempt {
            Objects.requireNonNull(outcome, "sample outcome");
            if (index <= 0 || completedAtNanos - startedAtNanos < 0) {
                throw new IllegalArgumentException("sample attempt needs a positive index and ordered times");
            }
        }

        long durationNanos() {
            return completedAtNanos - startedAtNanos;
        }
    }

    /** Counts describe completed attempts; a read still in progress is separate failure evidence. */
    record SamplingDiagnostics(String state, long attemptCount, long failureCount, long totalDurationNanos,
                               long maxDurationNanos, long omittedAttempts, List<Attempt> retainedAttempts,
                               int retainedAttemptLimit) {
        SamplingDiagnostics(String state, long attemptCount, long failureCount, long totalDurationNanos,
                            long maxDurationNanos, long omittedAttempts, List<Attempt> retainedAttempts) {
            this(state, attemptCount, failureCount, totalDurationNanos, maxDurationNanos, omittedAttempts,
                    retainedAttempts, MAX_RETAINED_ATTEMPTS);
        }
        SamplingDiagnostics {
            retainedAttempts = List.copyOf(retainedAttempts);
        }
    }

    /** An unfinished read has a start but no invented completion, duration or failure outcome. */
    record PendingRead(long index, long startedAtNanos, boolean finalRead) { }

    private record PublishedDiagnostics(SamplingDiagnostics completed, Optional<PendingRead> pending) { }

    /** Keeps exact totals and bounded first/last attempts, including the final failed attempt. */
    static final class AttemptLog {
        private final int retention;
        AttemptLog() { this(MAX_RETAINED_ATTEMPTS); }
        AttemptLog(int retention) {
            if (retention < 2 || retention > 4096) { throw new IllegalArgumentException("resource trace retention must have a finite phase bound"); }
            this.retention = retention;
        }
        private final List<Attempt> first = new ArrayList<>();
        private final ArrayDeque<Attempt> last = new ArrayDeque<>();
        private long count;
        private long failures;
        private long totalDuration;
        private long maxDuration;

        void append(long startedAt, long completedAt, Outcome outcome, String failureType,
                    BenchmarkProcessProbe.Snapshot reading) {
            Attempt attempt = new Attempt(++count, startedAt, completedAt, outcome, failureType, reading);
            if (outcome != Outcome.SUCCESS) {
                failures++;
            }
            totalDuration = Math.addExact(totalDuration, attempt.durationNanos());
            maxDuration = Math.max(maxDuration, attempt.durationNanos());
            if (first.size() < retention / 2) {
                first.add(attempt);
            } else {
                if (last.size() == retention - retention / 2) {
                    last.removeFirst();
                }
                last.addLast(attempt);
            }
        }

        SamplingDiagnostics snapshot() {
            List<Attempt> retained = new ArrayList<>(first);
            retained.addAll(last);
            return new SamplingDiagnostics(count == 0 ? "NOT_STARTED" : failures == 0 ? "COMPLETE" : "FAILED",
                    count, failures, totalDuration, maxDuration, count - retained.size(), retained, retention);
        }
    }

    /** Resource peaks and counter endpoints cover every successful sample, including omitted diagnostics. */
    static final class ResourceTotals {
        private BenchmarkProcessProbe.Snapshot first;
        private BenchmarkProcessProbe.Snapshot last;
        private long peakHeap;
        private long peakRss;
        private int count;

        void add(BenchmarkProcessProbe.Snapshot reading) {
            if (reading == null || !reading.complete()) {
                throw new AssertionError("resource window contains an incomplete sample: " + reading);
            }
            if (first == null) {
                first = reading;
            }
            last = reading;
            count = Math.incrementExact(count);
            peakHeap = Math.max(peakHeap, reading.heapUsedBytes().orElseThrow());
            peakRss = Math.max(peakRss, reading.rssBytes().orElseThrow());
        }

        Summary summary(Optional<SamplingDiagnostics> diagnostics) {
            if (count < 2) {
                throw new AssertionError("resource window needs at least a start and end sample");
            }
            long cpu = last.cpuNanos().orElseThrow() - first.cpuNanos().orElseThrow();
            long gc = last.gcCollectionMillis().orElseThrow() - first.gcCollectionMillis().orElseThrow();
            if (cpu < 0 || gc < 0 || peakHeap <= 0 || peakRss <= 0) {
                throw new AssertionError("resource counter moved backward or memory was unavailable");
            }
            return new Summary(cpu, gc, peakHeap, peakRss, count, diagnostics);
        }
    }

    enum FailureStage {
        STATE,
        SHUTDOWN,
        FINAL_SAMPLE,
        READ,
        SUMMARY
    }

    enum FailureReason {
        NO_OPEN_WINDOW,
        SHUTDOWN_TIMEOUT,
        SHUTDOWN_INTERRUPTED,
        SHUTDOWN_ERROR,
        FINAL_SAMPLE_ERROR,
        READ_UNAVAILABLE,
        READ_ERROR,
        INVALID_SUMMARY
    }

    /** Window failures keep their stage separate from the count of completed reads that failed. */
    static final class SamplingFailure extends AssertionError {
        private final FailureStage stage;
        private final FailureReason reason;
        private final SamplingDiagnostics diagnostics;
        private final Optional<PendingRead> pendingRead;

        SamplingFailure(String phaseId, SamplingDiagnostics diagnostics, Throwable cause) {
            this(phaseId, FailureStage.READ, FailureReason.READ_ERROR, diagnostics, Optional.empty(), cause);
        }

        SamplingFailure(String phaseId, FailureStage stage, FailureReason reason,
                        SamplingDiagnostics diagnostics, Optional<PendingRead> pendingRead, Throwable cause) {
            super("external JVM resource sampling failed; phase=" + (phaseId == null ? "UNASSIGNED" : phaseId)
                    + "; stage=" + stage + "; reason=" + reason
                    + "; diagnostics=" + diagnostics + "; pendingRead=" + pendingRead, cause);
            this.stage = Objects.requireNonNull(stage, "failure stage");
            this.reason = Objects.requireNonNull(reason, "failure reason");
            this.diagnostics = diagnostics;
            this.pendingRead = pendingRead;
        }

        FailureStage stage() {
            return stage;
        }

        FailureReason reason() {
            return reason;
        }

        SamplingDiagnostics diagnostics() {
            return diagnostics;
        }

        Optional<PendingRead> pendingRead() {
            return pendingRead;
        }

        SamplingFailure inPhase(String phaseId) {
            return new SamplingFailure(Objects.requireNonNull(phaseId, "failed sampling phase"),
                    stage, reason, diagnostics, pendingRead, this);
        }
    }

    @Override
    public void close() {
        worker.shutdownNow();
        if (probe != null) {
            probe.close();
        }
    }
}
