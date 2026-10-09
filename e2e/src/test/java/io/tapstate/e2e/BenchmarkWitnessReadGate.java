package io.tapstate.e2e;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** A diagnostic reader may wait for its own measured ACK without changing the open cursor. */
final class BenchmarkWitnessReadGate {
    static final String PROPERTY = "tapstate.e2e.benchmark.witness-consumption-deferred";
    private final boolean deferred;
    private final long cursorOpenedAtNanos = System.nanoTime();
    private final CountDownLatch permission;
    private boolean closed;
    private Long ownAckAtNanos;
    private Long releasedAtNanos;
    private Long firstTryNextCallStartedAtNanos;
    private Map<String, Object> firstCompletedPhase;

    BenchmarkWitnessReadGate(boolean deferred) {
        this.deferred = deferred;
        permission = new CountDownLatch(deferred ? 1 : 0);
    }

    boolean awaitFirstRead() throws InterruptedException {
        permission.await();
        synchronized (this) {
            if (closed) { return false; }
            return true;
        }
    }

    synchronized boolean beforeFirstTryNext(long callStartedAtNanos) {
        if (closed) { return false; }
        if (firstTryNextCallStartedAtNanos != null || deferred && (releasedAtNanos == null
                || callStartedAtNanos < releasedAtNanos)) {
            throw new AssertionError("witness read preceded its permission or repeated its first read");
        }
        firstTryNextCallStartedAtNanos = callStartedAtNanos;
        return true;
    }

    synchronized void releaseAfterOwnAck(long acknowledgedAtNanos) {
        if (!deferred) { return; }
        long released = System.nanoTime();
        if (closed || ownAckAtNanos != null || acknowledgedAtNanos < cursorOpenedAtNanos
                || acknowledgedAtNanos > released) {
            throw new AssertionError("deferred witness requires its ordered, unrepeated own measured ACK");
        }
        ownAckAtNanos = acknowledgedAtNanos;
        releasedAtNanos = released;
        permission.countDown();
    }

    synchronized void close() {
        closed = true;
        permission.countDown();
    }

    boolean deferred() { return deferred; }

    synchronized void completedPhase(String phase, int expected, int observed) {
        if (expected != observed || firstTryNextCallStartedAtNanos == null) {
            throw new AssertionError("witness phase cannot complete without its full read coverage");
        }
        if (firstCompletedPhase == null) {
            firstCompletedPhase = Map.of("phase", phase, "expectedChanges", expected,
                    "observedChanges", observed, "completedAtNanos", System.nanoTime());
        }
    }

    synchronized Map<String, Object> evidence() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", deferred ? "DEFERRED_UNTIL_OWN_MEASURED_ACK" : "CONCURRENT");
        result.put("cursorOpenedAtNanos", cursorOpenedAtNanos);
        result.put("ownAckAtNanos", ownAckAtNanos);
        result.put("releasedAtNanos", releasedAtNanos);
        result.put("firstTryNextCallStartedAtNanos", firstTryNextCallStartedAtNanos);
        result.put("closed", closed);
        result.put("fullFirstPhaseDrainCompleted", firstCompletedPhase != null);
        result.put("firstCompletedPhase", firstCompletedPhase);
        result.put("localDeliveryLatencyPerformanceEligible", !deferred);
        result.put("formalCostAllowedByReadSchedule", !deferred);
        result.put("scope", "DRIVER_MONOTONIC_CALL_BOUNDARIES_NOT_MONGO_SERVICE_INTERVALS");
        return java.util.Collections.unmodifiableMap(result);
    }
}
