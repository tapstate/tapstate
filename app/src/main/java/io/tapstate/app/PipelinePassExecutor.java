package io.tapstate.app;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Bounded, queue-free execution with one complete convergence pass per Pipeline at a time. */
final class PipelinePassExecutor implements AutoCloseable {
    private final Set<String> active = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final ThreadPoolExecutor workers;
    private final Semaphore startSlots;
    private final Semaphore periodicSlots;

    PipelinePassExecutor(int parallelism) {
        if (parallelism < 2) throw new IllegalArgumentException("at least two convergence workers are required");
        // Neither lane may use the last slot needed by the other.
        startSlots = new Semaphore(parallelism - 1);
        periodicSlots = new Semaphore(parallelism - 1);
        var sequence = new AtomicInteger();
        workers = new ThreadPoolExecutor(parallelism, parallelism, 0, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(), task -> ShutdownAwait.daemon(task,
                        "pipeline-converge-" + sequence.incrementAndGet()), new ThreadPoolExecutor.AbortPolicy());
    }

    boolean submit(String pipeline, Runnable pass) {
        return submit(pipeline, pass, null, null);
    }

    boolean submitStart(String pipeline, Runnable pass) {
        return submit(pipeline, pass, startSlots, "reserved_for_periodic_work");
    }

    boolean submitPeriodic(String pipeline, Runnable pass) {
        return submit(pipeline, pass, periodicSlots, "reserved_for_start");
    }

    private boolean submit(String pipeline, Runnable pass, Semaphore lane, String reserved) {
        var timing = PipelineDiagnostics.attempt(pipeline);
        if (!reserve(pipeline)) { timing.admission(stopped.get() ? "stopped" : "in_flight", active); return false; }
        if (lane != null && !lane.tryAcquire()) {
            active.remove(pipeline);
            timing.admission(reserved, active);
            return false;
        }
        boolean accepted = false;
        try {
            workers.execute(() -> {
                try { runReserved(pipeline, () -> timing.run(pass)); }
                finally { if (lane != null) lane.release(); }
            });
            accepted = true;
            timing.admission("accepted", active);
            return true;
        } catch (RejectedExecutionException saturated) {
            timing.admission(stopped.get() ? "stopped" : "saturated", active);
            return false;
        } finally {
            if (!accepted) {
                active.remove(pipeline);
                if (lane != null) lane.release();
            }
        }
    }

    void runInline(String pipeline, Runnable pass) {
        var timing = PipelineDiagnostics.attempt(pipeline);
        if (reserve(pipeline)) { timing.admission("inline", active); runReserved(pipeline, () -> timing.run(pass)); }
        else timing.admission(stopped.get() ? "stopped" : "in_flight", active);
    }

    private boolean reserve(String pipeline) {
        if (stopped.get() || !active.add(pipeline)) return false;
        if (!stopped.get()) return true;
        active.remove(pipeline);
        return false;
    }

    private void runReserved(String pipeline, Runnable pass) {
        try { if (!stopped.get()) pass.run(); }
        finally { active.remove(pipeline); }
    }

    Set<String> activePipelines() { return Set.copyOf(active); }

    void stop() {
        stopped.set(true);
        workers.shutdownNow();
    }

    void awaitStopped(long deadline) { ShutdownAwait.executor(workers, deadline); }

    @Override public void close() { stop(); awaitStopped(ShutdownAwait.deadline()); }
}
