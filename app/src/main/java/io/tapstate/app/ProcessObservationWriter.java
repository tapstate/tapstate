package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
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
    private static final long HISTORY_ALLOWANCE_NANOS = Duration.ofSeconds(2).toNanos();
    private final MongoCacheDomain cache;
    private final Map<String, Stage> pending = new LinkedHashMap<>();
    private final ArrayDeque<History> pendingHistory = new ArrayDeque<>();
    private final Map<String, Long> historyAttempts = new LinkedHashMap<>();
    private final java.util.function.LongSupplier nanoTime;
    private long historySequence;
    private final ReentrantLock writing = new ReentrantLock();
    private final AtomicBoolean drainingHistory = new AtomicBoolean();
    private final ScheduledThreadPoolExecutor observationTimer;
    private final ScheduledThreadPoolExecutor historyTimer;
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
        observationTimer = timer("pipeline-observation-writer");
        historyTimer = timer("pipeline-history-writer");
        if (scheduled) {
            long delay = interval.toNanos();
            observationTimer.scheduleWithFixedDelay(this::drainObservations, delay, delay, TimeUnit.NANOSECONDS);
            historyTimer.scheduleWithFixedDelay(this::drainHistory, delay, delay, TimeUnit.NANOSECONDS);
        }
    }

    private static ScheduledThreadPoolExecutor timer(String name) {
        var timer = new ScheduledThreadPoolExecutor(1, task -> ShutdownAwait.daemon(task, name));
        timer.setRemoveOnCancelPolicy(true);
        return timer;
    }

    interface ActiveStore extends ObservationStore { Runnable writeGuard(); }

    Stage stage(PipelineConvergencePass pass) { return new Stage(pass); }

    record Usage(int pending, int inFlight, long retainedBytes) {}
    Usage usage() {
        synchronized (pending) { return new Usage(pending.size() + pendingHistory.size(), inFlight, retainedBytes); }
    }

    record History(String pipeline, Instant observedAt, long bytes, Runnable requireActive, Consumer<Duration> append) {
        History(RateSample sample, Consumer<Duration> append) {
            this(sample.pipelineId(), sample.observedAt(), historyBytes(sample), () -> {}, append);
        }

        History guardedBy(Runnable guard) {
            Runnable prior = requireActive;
            return new History(pipeline, observedAt, bytes, () -> { guard.run(); prior.run(); }, append);
        }
    }

    boolean submit(Stage stage, Runnable committed, Runnable failed) {
        return submit(stage, committed, null, failed);
    }

    boolean submit(Stage stage, Runnable committed, History sampled, Runnable failed) {
        if (stage.observation == null) return true;
        stage.committed.add(committed);
        stage.sampled = sampled == null ? null : sampled.guardedBy(stage.active);
        stage.failed = failed;
        stage.bytes = estimatedBytes(stage.observation) + (sampled == null ? 0 : sampled.bytes());
        if (stage.bytes > MAX_BYTES) {
            LOG.debug("Publishing a large observation synchronously for pipeline {}", stage.pipeline());
            now(stage.pipeline(), () -> {
                RuntimeException failure = cache.writeObservations(List.of(stage.write())).getFirst();
                if (failure != null) throw failure;
                stage.active.run();
                stage.committed.forEach(Runnable::run);
                offerHistory(stage.sampled);
            });
            return true;
        }
        boolean accepted;
        synchronized (pending) {
            Stage previous = pending.get(stage.pipeline());
            long nextBytes = retainedBytes - (previous == null ? 0 : previous.bytes) + stage.bytes;
            accepted = !closed && nextBytes <= MAX_BYTES
                    && (previous != null || pending.size() + pendingHistory.size() + inFlight < MAX_ENTRIES);
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

    /** Already committed synchronous publications hand off only their compact sample. */
    void offerHistory(History sample) {
        if (sample == null) return;
        synchronized (pending) {
            if (closed) return;
            if (pending.size() + pendingHistory.size() + inFlight < MAX_ENTRIES
                    && retainedBytes + sample.bytes() <= MAX_BYTES) {
                pendingHistory.addLast(sample);
                retainedBytes += sample.bytes();
                return;
            }
        }
        LOG.warn("History write capacity is exhausted for pipeline {}", sample.pipeline());
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
            var history = pendingHistory.iterator();
            while (history.hasNext()) {
                History sample = history.next();
                if (!pipelines.contains(sample.pipeline())) {
                    retainedBytes -= sample.bytes();
                    history.remove();
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
        drainObservations();
        drainHistory();
    }

    private void drainObservations() {
        if (!writing.tryLock()) return;
        List<Stage> batch = new ArrayList<>();
        List<History> history = new ArrayList<>();
        try {
            writeObservations(batch, history);
        } finally {
            try { handoffHistory(batch, history); }
            finally { writing.unlock(); }
        }
    }

    private void handoffHistory(List<Stage> batch, List<History> history) {
        synchronized (pending) {
            for (Stage stage : batch) retainedBytes -= stage.bytes;
            inFlight -= batch.size();
            if (closed) return;
            // Each completed Stage already reserved one entry and the bytes of its compact sample.
            for (History sample : history) {
                pendingHistory.addLast(sample);
                retainedBytes += sample.bytes();
            }
        }
    }

    private void writeObservations(List<Stage> batch, List<History> history) {
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
            inFlight += batch.size();
        }
        if (batch.isEmpty()) return;
        List<RuntimeException> results;
        try {
            results = cache.writeObservations(batch.stream().map(Stage::write).toList());
        } catch (RuntimeException failure) {
            results = java.util.Collections.nCopies(batch.size(), failure);
        }
        for (int index = 0; index < batch.size(); index++) {
            Stage stage = batch.get(index);
            try {
                RuntimeException failure = results.get(index);
                inPipeline(stage.pipeline(), () -> {
                    if (failure == null) {
                        stage.active.run();
                        stage.committed.forEach(Runnable::run);
                        if (stage.sampled != null) history.add(stage.sampled);
                    } else {
                        LOG.warn("Could not publish the observation of pipeline {}", stage.pipeline(), failure);
                        stage.failed.run();
                    }
                });
            } catch (RuntimeException failure) {
                LOG.warn("Observation follow-up failed for pipeline {}", stage.pipeline(), failure);
            }
        }
    }

    private void drainHistory() {
        if (!drainingHistory.compareAndSet(false, true)) return;
        try {
            long deadline = nanoTime.getAsLong() + HISTORY_ALLOWANCE_NANOS;
            for (int index = 0; index < BATCH_ENTRIES && deadline - nanoTime.getAsLong() > 0; index++) {
                History sample = takeHistory();
                if (sample == null) return;
                appendHistory(sample, deadline);
            }
        } finally {
            drainingHistory.set(false);
        }
    }

    private void appendHistory(History sample, long deadline) {
        try {
            if (rememberHistoryAttempt(sample.pipeline())) inPipeline(sample.pipeline(), () -> {
                sample.requireActive().run();
                long remaining = deadline - nanoTime.getAsLong();
                if (remaining > 0) sample.append().accept(Duration.ofNanos(remaining));
            });
        } catch (RuntimeException failure) {
            LOG.warn("Could not sample the history of pipeline {}", sample.pipeline(), failure);
        } finally {
            synchronized (pending) {
                retainedBytes -= sample.bytes();
                inFlight--;
            }
        }
    }

    private History takeHistory() {
        synchronized (pending) {
            if (closed) return null;
            History first = null;
            for (History sample : pendingHistory) {
                if (first == null || compareHistory(sample, first) < 0) first = sample;
            }
            if (first != null) {
                pendingHistory.remove(first);
                inFlight++;
            }
            return first;
        }
    }

    private int compareHistory(History left, History right) {
        int attempt = Long.compare(historyAttempts.getOrDefault(left.pipeline(), Long.MIN_VALUE),
                historyAttempts.getOrDefault(right.pipeline(), Long.MIN_VALUE));
        return attempt != 0 ? attempt : left.observedAt().compareTo(right.observedAt());
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

    private static long historyBytes(RateSample sample) {
        long bytes = 512 + stringBytes(sample.pipelineId());
        for (String counter : sample.counters().keySet()) bytes += 96 + stringBytes(counter);
        for (String table : sample.lag().keySet()) bytes += 96 + stringBytes(table);
        return bytes * 2;
    }

    @Override public void close() {
        synchronized (pending) {
            closed = true;
            for (Stage stage : pending.values()) retainedBytes -= stage.bytes;
            pending.clear();
            for (History sample : pendingHistory) retainedBytes -= sample.bytes();
            pendingHistory.clear();
            for (String pipeline : historyAttempts.keySet()) retainedBytes -= historyBytes(pipeline);
            historyAttempts.clear();
        }
        observationTimer.shutdown();
        historyTimer.shutdown();
        long deadline = ShutdownAwait.deadline();
        ShutdownAwait.executor(observationTimer, deadline);
        ShutdownAwait.executor(historyTimer, deadline);
    }

    final class Stage {
        private final PipelineConvergencePass pass;
        private final WorkloadClaimFence claim;
        private final List<Runnable> committed = new ArrayList<>();
        private Observation observation;
        private long epoch;
        private long bytes;
        private Runnable failed = () -> {};
        private History sampled;
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
            Runnable measured = destination instanceof ActiveStore guarded ? guarded.writeGuard() : () -> {};
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
