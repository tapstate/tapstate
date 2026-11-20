package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** One bounded process-local entrance for latest observations and their committed side effects. */
final class ProcessObservationWriter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessObservationWriter.class);
    private static final int MAX_ENTRIES = 1024;
    private static final long MAX_BYTES = 16L * 1024 * 1024;
    private static final int BATCH_ENTRIES = 32;
    private static final long BATCH_BYTES = 4L * 1024 * 1024;
    private final MongoCacheDomain cache;
    private final Map<String, Stage> pending = new LinkedHashMap<>();
    private final Map<String, Long> historyAttempts = new LinkedHashMap<>();
    private final java.util.function.LongSupplier nanoTime;
    private long historySequence;
    private final ReentrantLock writing = new ReentrantLock();
    private final ScheduledThreadPoolExecutor timer;
    private long retainedBytes;
    private int inFlight;
    private boolean closed;

    ProcessObservationWriter(MongoCacheDomain cache, Duration interval) { this(cache, interval, true, System::nanoTime); }

    ProcessObservationWriter(MongoCacheDomain cache, boolean scheduled) { this(cache, Duration.ofSeconds(1), scheduled, System::nanoTime); }

    ProcessObservationWriter(MongoCacheDomain cache, boolean scheduled, java.util.function.LongSupplier nanoTime) {
        this(cache, Duration.ofSeconds(1), scheduled, nanoTime);
    }

    private ProcessObservationWriter(MongoCacheDomain cache, Duration interval, boolean scheduled,
            java.util.function.LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
        this.cache = java.util.Objects.requireNonNull(cache, "cache");
        timer = new ScheduledThreadPoolExecutor(1, task -> ShutdownAwait.daemon(task, "pipeline-observation-writer"));
        timer.setRemoveOnCancelPolicy(true);
        if (scheduled) timer.scheduleWithFixedDelay(this::flush, interval.toNanos(), interval.toNanos(), TimeUnit.NANOSECONDS);
    }

    interface ActiveStore extends ObservationStore { void requireWriteActive(); }

    Stage stage(PipelineConvergencePass pass) { return new Stage(pass); }

    record Usage(int pending, int inFlight, long retainedBytes) {}
    Usage usage() {
        synchronized (pending) { return new Usage(pending.size(), inFlight, retainedBytes); }
    }

    boolean submit(Stage stage, Runnable committed, Runnable failed) {
        return submit(stage, committed, null, failed);
    }

    boolean submit(Stage stage, Runnable committed, Runnable sampled, Runnable failed) {
        if (stage.observation == null) return true;
        stage.committed.add(committed);
        stage.sampled = sampled;
        stage.failed = failed;
        stage.bytes = estimatedBytes(stage.observation);
        if (stage.bytes > MAX_BYTES) {
            LOG.debug("Publishing a large observation synchronously for pipeline {}", stage.pipeline());
            now(stage.pipeline(), () -> {
                RuntimeException failure = cache.writeObservations(List.of(stage.write())).getFirst();
                if (failure != null) throw failure;
                stage.active.run();
                stage.committed.forEach(Runnable::run);
                if (sampled != null) sampled.run();
            });
            return true;
        }
        boolean accepted;
        synchronized (pending) {
            Stage previous = pending.get(stage.pipeline());
            long nextBytes = retainedBytes - (previous == null ? 0 : previous.bytes) + stage.bytes;
            accepted = !closed && nextBytes <= MAX_BYTES
                    && (previous != null || pending.size() + inFlight < MAX_ENTRIES);
            if (accepted) {
                pending.put(stage.pipeline(), stage);
                retainedBytes = nextBytes;
            }
        }
        if (!accepted) {
            LOG.warn("Observation write capacity is exhausted for pipeline {}; the next pass will provide its latest facts", stage.pipeline());
            failed.run();
        }
        return accepted;
    }

    /** Lifecycle barriers use the same entrance and finish before their checkpoint transition proceeds. */
    void now(String pipeline, Runnable publication) {
        writing.lock();
        try {
            synchronized (pending) {
                Stage removed = pending.remove(pipeline);
                if (removed != null) retainedBytes -= removed.bytes;
                if (closed) throw StorageRuntime.unavailable();
            }
            inPipeline(pipeline, publication);
        } finally {
            writing.unlock();
        }
    }

    void retain(Collection<String> pipelines) {
        synchronized (pending) {
            var iterator = pending.entrySet().iterator();
            while (iterator.hasNext()) {
                Stage stage = iterator.next().getValue();
                if (!pipelines.contains(stage.pipeline())) {
                    retainedBytes -= stage.bytes;
                    iterator.remove();
                }
            }
            var attempts = historyAttempts.keySet().iterator();
            while (attempts.hasNext()) {
                String pipeline = attempts.next();
                if (!pipelines.contains(pipeline)) {
                    retainedBytes -= historyBytes(pipeline);
                    attempts.remove();
                }
            }
        }
    }

    void flush() {
        if (!writing.tryLock()) return;
        List<Stage> batch = new ArrayList<>();
        try {
            synchronized (pending) {
                if (closed) return;
                long bytes = 0;
                var iterator = pending.values().iterator();
                while (iterator.hasNext() && batch.size() < BATCH_ENTRIES) {
                    Stage next = iterator.next();
                    if (!batch.isEmpty() && bytes + next.bytes > BATCH_BYTES) break;
                    batch.add(next);
                    bytes += next.bytes;
                    iterator.remove();
                }
                inFlight = batch.size();
            }
            if (batch.isEmpty()) return;
            List<RuntimeException> results;
            try {
                results = cache.writeObservations(batch.stream().map(Stage::write).toList());
            } catch (RuntimeException failure) {
                results = java.util.Collections.nCopies(batch.size(), failure);
            }
            var history = new ArrayList<Stage>();
            for (int index = 0; index < batch.size(); index++) {
                Stage stage = batch.get(index);
                try {
                    RuntimeException failure = results.get(index);
                    inPipeline(stage.pipeline(), () -> {
                        if (failure == null) {
                            stage.active.run();
                            stage.committed.forEach(Runnable::run);
                            if (stage.sampled != null) history.add(stage);
                        } else {
                            LOG.warn("Could not publish the observation of pipeline {}", stage.pipeline(), failure);
                            stage.failed.run();
                        }
                    });
                } catch (RuntimeException failure) {
                    LOG.warn("Observation follow-up failed for pipeline {}", stage.pipeline(), failure);
                }
            }
            synchronized (pending) {
                history.sort(java.util.Comparator.comparingLong(stage -> historyAttempt(stage.pipeline())));
            }
            long historyDeadline = nanoTime.getAsLong() + Duration.ofSeconds(2).toNanos();
            for (Stage stage : history) {
                if (nanoTime.getAsLong() >= historyDeadline) break;
                if (!rememberHistoryAttempt(stage.pipeline())) continue;
                try { inPipeline(stage.pipeline(), stage.sampled); }
                catch (RuntimeException failure) { LOG.warn("Could not sample the history of pipeline {}", stage.pipeline(), failure); }
            }
        } finally {
            synchronized (pending) {
                for (Stage stage : batch) retainedBytes -= stage.bytes;
                inFlight = 0;
            }
            writing.unlock();
        }
    }

    private static void inPipeline(String pipeline, Runnable action) {
        String previous = MDC.get(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
        MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, pipeline);
        try { action.run(); }
        finally {
            if (previous == null) MDC.remove(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
            else MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, previous);
        }
    }

    private long historyAttempt(String pipeline) {
        synchronized (pending) { return historyAttempts.getOrDefault(pipeline, Long.MIN_VALUE); }
    }

    private boolean rememberHistoryAttempt(String pipeline) {
        synchronized (pending) {
            if (historyAttempts.remove(pipeline) != null) retainedBytes -= historyBytes(pipeline);
            long bytes = historyBytes(pipeline);
            while (!historyAttempts.isEmpty() && (historyAttempts.size() >= MAX_ENTRIES || retainedBytes + bytes > MAX_BYTES)) {
                String oldest = historyAttempts.keySet().iterator().next();
                historyAttempts.remove(oldest);
                retainedBytes -= historyBytes(oldest);
            }
            if (closed || retainedBytes + bytes > MAX_BYTES) return false;
            historyAttempts.put(pipeline, ++historySequence);
            retainedBytes += bytes;
            return true;
        }
    }

    private static long historyBytes(String pipeline) { return 96 + stringBytes(pipeline); }

    @Override public void close() {
        synchronized (pending) {
            closed = true;
            for (Stage stage : pending.values()) retainedBytes -= stage.bytes;
            pending.clear();
            for (String pipeline : historyAttempts.keySet()) retainedBytes -= historyBytes(pipeline);
            historyAttempts.clear();
        }
        timer.shutdown();
        ShutdownAwait.executor(timer, System.nanoTime() + Duration.ofSeconds(10).toNanos());
    }

    final class Stage {
        private final PipelineConvergencePass pass;
        private final WorkloadClaimFence claim;
        private final List<Runnable> committed = new ArrayList<>();
        private Observation observation;
        private long epoch;
        private long bytes;
        private Runnable failed = () -> {};
        private Runnable sampled;
        private Runnable active;

        private Stage(PipelineConvergencePass pass) {
            this.pass = pass;
            this.claim = pass.claim();
            this.active = pass.guardOriginal(claim);
        }

        String pipeline() { return pass.scope().pipelineId(); }

        MongoCacheDomain.ObservationWrite write() {
            return new MongoCacheDomain.ObservationWrite(pass.scope(), claim, active, observation, epoch);
        }

        ObservationStore wrap(ObservationStore destination) {
            Runnable original = pass.guardOriginal(claim);
            Runnable measured = destination instanceof ActiveStore guarded ? guarded::requireWriteActive : () -> {};
            return new ObservationStore() {
                @Override public void save(Observation value) { throw new IllegalArgumentException("an observation requires its measured checkpoint epoch"); }
                @Override public void save(Observation value, long checkpointEpoch) {
                    observation = value;
                    epoch = checkpointEpoch;
                    active = () -> { original.run(); measured.run(); };
                }
                @Override public void afterSave(Runnable action) { committed.add(action); }
                @Override public Optional<Observation> read(String pipeline) { return destination.read(pipeline); }
                @Override public Optional<Observation> beforeCompletion(String pipeline, long checkpointEpoch) {
                    return destination.beforeCompletion(pipeline, checkpointEpoch);
                }
                @Override public void delete(String pipeline) { destination.delete(pipeline); }
            };
        }
    }

    /** Includes retained object/entry overhead and a second encoding-sized allowance for the active batch. */
    static long estimatedBytes(Observation value) {
        long bytes = 1024 + stringBytes(value.pipelineId());
        for (var entry : value.metrics().entrySet()) bytes += 96 + stringBytes(entry.getKey());
        for (var entry : value.snapshot().entrySet()) bytes += 160 + stringBytes(entry.getKey());
        bytes += mapBytes(value.positions());
        if (value.failure() != null) bytes += 128 + stringBytes(value.failure().code()) + mapBytes(value.failure().params());
        if (value.initialLoad() != null) {
            bytes += 160 + stringBytes(value.initialLoad().runKey()) + stringBytes(value.initialLoad().readMode());
            for (var key : value.initialLoad().tables().keySet()) bytes += 160 + stringBytes(key);
        }
        for (var fact : value.facts()) {
            bytes += 128 + stringBytes(fact.name()) + stringBytes(fact.unit());
            for (var point : fact.points()) {
                bytes += 192 + mapBytes(point.attributes());
                if (point.histogram() != null)
                    bytes += 96 + 32L * (point.histogram().bounds().size() + point.histogram().bucketCounts().size());
            }
        }
        return bytes * 2;
    }

    private static long mapBytes(Map<String, String> values) {
        long bytes = 64;
        for (var entry : values.entrySet()) bytes += 64 + stringBytes(entry.getKey()) + stringBytes(entry.getValue());
        return bytes;
    }

    private static long stringBytes(String value) { return 48L + 2L * value.length(); }
}
