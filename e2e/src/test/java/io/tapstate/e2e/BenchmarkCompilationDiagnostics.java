package io.tapstate.e2e;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Independently brackets optional compilation reads on the caller's existing sampling worker. */
final class BenchmarkCompilationDiagnostics {
    private final Supplier<BenchmarkProcessProbe.CompilationReading> reader;
    private final LongSupplier nanoTime;
    private final Object lifecycle = new Object();
    private final int retention;
    private final List<Attempt> first = new ArrayList<>();
    private final ArrayDeque<Attempt> last = new ArrayDeque<>();
    private volatile Evidence published;
    private boolean reading;
    private Long previousCounter;

    private BenchmarkCompilationDiagnostics(Supplier<BenchmarkProcessProbe.CompilationReading> reader,
                                            LongSupplier nanoTime, int retention) {
        this.reader = Objects.requireNonNull(reader, "compilation reader");
        this.nanoTime = Objects.requireNonNull(nanoTime, "compilation read clock");
        if (retention < 2 || retention > 4096) {
            throw new IllegalArgumentException("compilation trace retention must be between two and 4096");
        }
        this.retention = retention;
        published = new Evidence(0, 0, 0, 0, 0, 0, retention, List.of(), Optional.empty());
    }

    static BenchmarkCompilationDiagnostics from(Supplier<BenchmarkProcessProbe.CompilationReading> reader,
                                               LongSupplier nanoTime, int retention) {
        return new BenchmarkCompilationDiagnostics(reader, nanoTime, retention);
    }

    Attempt record() {
        long index;
        long startedAt;
        synchronized (lifecycle) {
            if (reading || published.pending().isPresent()) {
                throw new IllegalStateException("compilation read is already pending");
            }
            startedAt = nanoTime.getAsLong();
            index = Math.addExact(published.attemptCount(), 1);
            reading = true;
            published = withPending(published, new PendingRead(index, startedAt));
        }
        BenchmarkProcessProbe.CompilationReading value = null;
        Outcome outcome = Outcome.SUCCESS;
        String failureType = null;
        Error invariantFailure = null;
        try {
            value = reader.get();
            if (value == null) {
                outcome = Outcome.ERROR;
                failureType = "NULL_READING";
            } else if (value.state() == BenchmarkProcessProbe.CompilationState.UNKNOWN) {
                outcome = Outcome.UNKNOWN;
            } else if (previousCounter != null && value.totalCompilationMillis().getAsLong() < previousCounter) {
                outcome = Outcome.UNKNOWN;
                failureType = "COUNTER_DECREASED";
            } else {
                previousCounter = value.totalCompilationMillis().getAsLong();
            }
        } catch (RuntimeException failure) {
            outcome = Outcome.ERROR;
            failureType = failure.getClass().getName();
        } catch (Error failure) {
            outcome = Outcome.ERROR;
            failureType = failure.getClass().getName();
            invariantFailure = failure;
        }
        Attempt attempt;
        try {
            long completedAt = nanoTime.getAsLong();
            attempt = new Attempt(index, startedAt, completedAt, outcome, value, failureType);
            synchronized (lifecycle) {
                retain(attempt);
                Evidence before = published;
                List<Attempt> retained = new ArrayList<>(first);
                retained.addAll(last);
                published = new Evidence(index, before.unknownCount() + (outcome == Outcome.UNKNOWN ? 1 : 0),
                        before.errorCount() + (outcome == Outcome.ERROR ? 1 : 0),
                        Math.addExact(before.totalDurationNanos(), attempt.durationNanos()),
                        Math.max(before.maxDurationNanos(), attempt.durationNanos()), index - retained.size(),
                        retention, retained, Optional.empty());
            }
        } finally {
            synchronized (lifecycle) { reading = false; }
        }
        if (invariantFailure != null) { throw invariantFailure; }
        return attempt;
    }

    /** Never waits for the reader or its locks, including when an external read is unfinished. */
    Evidence evidence() { return published; }

    Map<String, Object> wireEvidence() {
        Evidence snapshot = published;
        var result = new LinkedHashMap<String, Object>();
        result.put("state", snapshot.pending().isPresent() || snapshot.unknownCount() > 0 || snapshot.errorCount() > 0
                || snapshot.omittedAttempts() > 0 ? "UNKNOWN" : snapshot.attemptCount() == 0 ? "NOT_RECORDED" : "RECORDED");
        result.put("counterScope", "APPROXIMATE_CUMULATIVE_COMPILATION_ELAPSED_MILLIS");
        result.put("aggregation", "SUM_OF_REPORTED_COMPILER_THREAD_ELAPSED_TIMES");
        result.put("notCpuOrPauseTime", true);
        result.put("clockScope", "DRIVER_MONOTONIC_REQUEST_BRACKET");
        result.put("performanceAcceptanceEligible", false);
        result.put("attemptCount", snapshot.attemptCount());
        result.put("unknownCount", snapshot.unknownCount());
        result.put("errorCount", snapshot.errorCount());
        result.put("totalDurationNanos", snapshot.totalDurationNanos());
        result.put("maxDurationNanos", snapshot.maxDurationNanos());
        result.put("omittedAttempts", snapshot.omittedAttempts());
        result.put("retainedAttemptLimit", snapshot.retainedAttemptLimit());
        result.put("retention", "FIRST_AND_LAST");
        result.put("attempts", snapshot.retainedAttempts().stream().map(BenchmarkCompilationDiagnostics::wireAttempt).toList());
        snapshot.pending().ifPresent(pending -> result.put("pending", Map.of("index", pending.index(),
                "startedAtNanos", pending.startedAtNanos())));
        return java.util.Collections.unmodifiableMap(result);
    }

    private static Map<String, Object> wireAttempt(Attempt attempt) {
        var result = new LinkedHashMap<String, Object>();
        result.put("index", attempt.index());
        result.put("startedAtNanos", attempt.startedAtNanos());
        result.put("completedAtNanos", attempt.completedAtNanos());
        result.put("durationNanos", attempt.durationNanos());
        result.put("outcome", attempt.outcome().name());
        if (attempt.failureType() != null) { result.put("failureType", attempt.failureType()); }
        if (attempt.reading() != null) {
            var reading = attempt.reading();
            var values = new LinkedHashMap<String, Object>();
            values.put("ownedPid", reading.ownedPid());
            values.put("state", reading.state().name());
            values.put("unknownReason", reading.unknownReason().name());
            if (reading.compilerName() != null) { values.put("compilerName", reading.compilerName()); }
            if (reading.monitoringSupported() != null) { values.put("monitoringSupported", reading.monitoringSupported()); }
            if (reading.totalCompilationMillis().isPresent()) {
                values.put("totalCompilationMillis", reading.totalCompilationMillis().getAsLong());
            }
            if (reading.failureType() != null) { values.put("failureType", reading.failureType()); }
            result.put("reading", java.util.Collections.unmodifiableMap(values));
        }
        return java.util.Collections.unmodifiableMap(result);
    }

    private void retain(Attempt attempt) {
        if (first.size() < (retention + 1) / 2) { first.add(attempt); }
        else {
            last.addLast(attempt);
            if (last.size() > retention / 2) { last.removeFirst(); }
        }
    }

    private static Evidence withPending(Evidence before, PendingRead pending) {
        return new Evidence(before.attemptCount(), before.unknownCount(), before.errorCount(),
                before.totalDurationNanos(), before.maxDurationNanos(), before.omittedAttempts(),
                before.retainedAttemptLimit(), before.retainedAttempts(), Optional.of(pending));
    }

    enum Outcome { SUCCESS, UNKNOWN, ERROR }

    record Attempt(long index, long startedAtNanos, long completedAtNanos, Outcome outcome,
                   BenchmarkProcessProbe.CompilationReading reading, String failureType) {
        Attempt {
            Objects.requireNonNull(outcome, "compilation read outcome");
            if (index <= 0 || completedAtNanos - startedAtNanos < 0) {
                throw new IllegalArgumentException("compilation read needs a positive index and ordered times");
            }
        }
        long durationNanos() { return completedAtNanos - startedAtNanos; }
    }

    record PendingRead(long index, long startedAtNanos) { }

    record Evidence(long attemptCount, long unknownCount, long errorCount, long totalDurationNanos,
                    long maxDurationNanos, long omittedAttempts, int retainedAttemptLimit,
                    List<Attempt> retainedAttempts, Optional<PendingRead> pending) {
        Evidence {
            retainedAttempts = List.copyOf(retainedAttempts);
            Objects.requireNonNull(pending, "pending compilation read");
        }
    }
}
