package io.tapstate.app;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
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

    PipelinePassExecutor(int parallelism) {
        if (parallelism < 2) throw new IllegalArgumentException("at least two convergence workers are required");
        var sequence = new AtomicInteger();
        workers = new ThreadPoolExecutor(parallelism, parallelism, 0, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(), task -> ShutdownAwait.daemon(task,
                        "pipeline-converge-" + sequence.incrementAndGet()), new ThreadPoolExecutor.AbortPolicy());
    }

    boolean submit(String pipeline, Runnable pass) {
        var timing = PipelineDiagnostics.attempt(pipeline);
        if (!reserve(pipeline)) { timing.admission(stopped.get() ? "stopped" : "in_flight", active); return false; }
        try {
            workers.execute(() -> runReserved(pipeline, () -> timing.run(pass)));
            timing.admission("accepted", active);
            return true;
        } catch (RejectedExecutionException saturated) {
            active.remove(pipeline);
            timing.admission(stopped.get() ? "stopped" : "saturated", active);
            return false;
        } catch (RuntimeException | Error failure) {
            active.remove(pipeline);
            throw failure;
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
