package io.tapstate.e2e;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;

/** Serial owned reads; actual brackets never imply equal clock origins, continuous drift or qualified cost. */
final class BenchmarkReturnClockSampler implements AutoCloseable {
    enum Mode { PERIODIC, FIRST_FINAL_CONTROL }

    static final Duration SAMPLE_DELAY = Duration.ofMillis(50);
    static final int MAX_SAMPLES = 512;
    private static final long OWNER_WAIT_NANOS = Duration.ofSeconds(2).toNanos();

    private final Object lock = new Object();
    private final LongFunction<BenchmarkCausalClock.Sample> reader;
    private final Mode mode;
    private final long delayNanos;
    private final List<BenchmarkCausalClock.Sample> readings = new ArrayList<>();
    private Thread worker;
    private boolean started;
    private boolean firstRecorded;
    private boolean finishRequested;
    private boolean finalRecorded;
    private boolean abort;
    private boolean workerExited;
    private boolean closed;
    private boolean inFlight;
    private String unknownReason;
    private Throwable failure;

    BenchmarkReturnClockSampler(LongFunction<BenchmarkCausalClock.Sample> reader) {
        this(reader, Mode.PERIODIC, SAMPLE_DELAY);
    }

    BenchmarkReturnClockSampler(LongFunction<BenchmarkCausalClock.Sample> reader, Mode mode) {
        this(reader, mode, SAMPLE_DELAY);
    }

    /** Cadence injection is confined to controls; actual callers use the fixed-delay constructor. */
    BenchmarkReturnClockSampler(LongFunction<BenchmarkCausalClock.Sample> reader, Duration testDelay) {
        this(reader, Mode.PERIODIC, testDelay);
    }

    /** A test delay never enables periodic requests in the explicit first/final control. */
    BenchmarkReturnClockSampler(LongFunction<BenchmarkCausalClock.Sample> reader, Mode mode, Duration testDelay) {
        this.reader = java.util.Objects.requireNonNull(reader);
        this.mode = java.util.Objects.requireNonNull(mode);
        java.util.Objects.requireNonNull(testDelay);
        if (testDelay.isNegative()) { throw new AssertionError("return clock delay is invalid"); }
        try { delayNanos = testDelay.toNanos(); }
        catch (ArithmeticException invalid) { throw new AssertionError("return clock delay exceeds its bound", invalid); }
    }

    /** Returns only after the worker retained its actual first reading, before the owner begins measurement. */
    void start() {
        synchronized (lock) {
            require(!started && !closed, "sampler already started or closed");
            started = true;
            worker = new Thread(this::run, "benchmark-return-clock");
            worker.setDaemon(true);
            worker.start();
            await(false, "FIRST_SAMPLE_TIMEOUT");
            requireKnown();
        }
    }

    /** The owner must have successfully stopped capture before requesting this distinct final read. */
    List<BenchmarkCausalClock.Sample> finishAfterSuccessfulStop() {
        synchronized (lock) {
            require(started && !closed, "sampler is not open");
            finishRequested = true;
            lock.notifyAll();
            await(true, "FINAL_SAMPLE_TIMEOUT");
            closed = true;
            requireKnown();
            require(finalRecorded && workerExited && !inFlight, "final coverage or worker completion is missing");
            return List.copyOf(readings);
        }
    }

    List<BenchmarkCausalClock.Sample> readings() {
        synchronized (lock) { return List.copyOf(readings); }
    }

    Mode mode() { return mode; }

    Map<String, Object> evidence() {
        synchronized (lock) {
            var evidence = new LinkedHashMap<String, Object>();
            evidence.put("state", unknownReason == null ? "RECORDED" : "UNKNOWN");
            evidence.put("reason", unknownReason == null ? (mode == Mode.PERIODIC
                    ? "BOUNDED_REQUEST_SAMPLES_ONLY" : "FIRST_AND_FINAL_ACTUAL_REQUEST_SAMPLES_ONLY") : unknownReason);
            evidence.put("mode", mode.name());
            evidence.put("periodicPollingEnabled", mode == Mode.PERIODIC);
            if (mode == Mode.PERIODIC) { evidence.put("fixedDelayNanos", delayNanos); }
            evidence.put("samples", readings.size()); evidence.put("firstRecorded", firstRecorded);
            evidence.put("finalRecorded", finalRecorded); evidence.put("workerExited", workerExited);
            evidence.put("performanceAcceptanceEligible", false); evidence.put("samplingCostQualified", false);
            return Map.copyOf(evidence);
        }
    }

    private void run() {
        try {
            if (!read(false)) { return; }
            synchronized (lock) { firstRecorded = true; lock.notifyAll(); }
            while (true) {
                boolean finalRead;
                synchronized (lock) {
                    if (mode == Mode.FIRST_FINAL_CONTROL) {
                        while (!abort && !finishRequested) { lock.wait(); }
                    } else {
                        long waitingSince = System.nanoTime();
                        while (!abort && !finishRequested) {
                            long remaining = delayNanos - (System.nanoTime() - waitingSince);
                            if (remaining <= 0) { break; }
                            TimeUnit.NANOSECONDS.timedWait(lock, remaining);
                        }
                    }
                    if (abort) { return; }
                    finalRead = finishRequested;
                }
                if (!read(finalRead) || finalRead) { return; }
            }
        } catch (Throwable unavailable) {
            synchronized (lock) {
                unknown("READER_OR_WORKER_FAILED", unavailable);
                abort = true;
            }
        } finally {
            synchronized (lock) { inFlight = false; workerExited = true; lock.notifyAll(); }
        }
    }

    private boolean read(boolean finalRead) {
        long sequence;
        synchronized (lock) {
            if (abort) { return false; }
            if (readings.size() >= MAX_SAMPLES) { unknown("SAMPLE_CAPACITY_EXHAUSTED", null); return false; }
            sequence = readings.size();
            inFlight = true;
        }
        BenchmarkCausalClock.Sample sample = reader.apply(sequence);
        synchronized (lock) {
            inFlight = false;
            if (abort) { return false; }
            validate(sample, sequence);
            readings.add(sample);
            if (finalRead) { finalRecorded = true; }
            else if (readings.size() == MAX_SAMPLES) { unknown("SAMPLE_CAPACITY_EXHAUSTED", null); return false; }
            lock.notifyAll();
            return true;
        }
    }

    private void validate(BenchmarkCausalClock.Sample sample, long sequence) {
        require(sample != null && sample.sequence() == sequence, "actual sample is missing or has another sequence");
        if (readings.isEmpty()) { return; }
        var previous = readings.getLast();
        require(readings.getFirst().identity().equals(sample.identity()), "owned runtime identity changed");
        require(Math.subtractExact(sample.driverBeforeNanos(), previous.driverAfterNanos()) >= 0,
                "actual request brackets are not serial");
        require(Math.subtractExact(sample.ownedNanos(), previous.ownedNanos()) >= 0, "owned clock moved backward");
        Math.subtractExact(sample.driverAfterNanos(), readings.getFirst().driverBeforeNanos());
        Math.subtractExact(sample.ownedNanos(), readings.getFirst().ownedNanos());
    }

    /** Waits on owner state only; a blocked borrowed reader is never closed or replaced. */
    private void await(boolean completion, String timeoutReason) {
        long startedWaiting = System.nanoTime();
        while ((completion ? !workerExited : !firstRecorded) && unknownReason == null) {
            long remaining = OWNER_WAIT_NANOS - (System.nanoTime() - startedWaiting);
            if (remaining <= 0) {
                unknown(timeoutReason, null); abort = true; worker.interrupt(); lock.notifyAll(); return;
            }
            try { TimeUnit.NANOSECONDS.timedWait(lock, remaining); }
            catch (InterruptedException interrupted) {
                unknown("OWNER_INTERRUPTED", interrupted); abort = true;
                worker.interrupt(); lock.notifyAll(); Thread.currentThread().interrupt(); return;
            }
        }
    }

    @Override public void close() {
        synchronized (lock) {
            if (closed && workerExited) { requireKnown(); return; }
            closed = true;
            if (!started) { return; }
            if (!finalRecorded) { unknown("ABORTED_BEFORE_SUCCESSFUL_STOP", null); }
            abort = true;
            worker.interrupt();
            lock.notifyAll();
            long startedWaiting = System.nanoTime();
            while (!workerExited) {
                long remaining = OWNER_WAIT_NANOS - (System.nanoTime() - startedWaiting);
                if (remaining <= 0) {
                    unknown("CLOSE_TIMEOUT", null);
                    throw new AssertionError("return clock UNKNOWN: owned read remains stuck after bounded close");
                }
                try { TimeUnit.NANOSECONDS.timedWait(lock, remaining); }
                catch (InterruptedException interrupted) {
                    unknown("OWNER_INTERRUPTED", interrupted); Thread.currentThread().interrupt();
                    throw new AssertionError("return clock UNKNOWN: close was interrupted", interrupted);
                }
            }
            requireKnown();
        }
    }

    private void unknown(String reason, Throwable cause) {
        if (unknownReason == null) { unknownReason = reason; failure = cause; }
        lock.notifyAll();
    }
    private void requireKnown() {
        if (unknownReason != null) { throw new AssertionError("return clock UNKNOWN: " + unknownReason, failure); }
    }
    private static void require(boolean condition, String reason) {
        if (!condition) { throw new AssertionError("return clock " + reason); }
    }
}
