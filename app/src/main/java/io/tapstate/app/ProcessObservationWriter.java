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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** One bounded process-local entrance for latest observations and their committed side effects. */
final class ProcessObservationWriter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessObservationWriter.class);
    private static final int MAX_ENTRIES = 1024;
    private static final long MAX_BYTES = 16L * 1024 * 1024;
    private static final int MAX_HISTORY_ENTRIES = MAX_ENTRIES - 256;
    private static final long MAX_HISTORY_BYTES = MAX_BYTES - 4L * 1024 * 1024;
    private static final long BARRIER_WAIT_NANOS = Duration.ofSeconds(10).toNanos();
    private static final int BATCH_ENTRIES = 32;
    private static final long BATCH_BYTES = 4L * 1024 * 1024;
    private static final long HISTORY_ALLOWANCE_NANOS = Duration.ofSeconds(2).toNanos();
    private final MongoCacheDomain cache;
    private final Map<String, Stage> pending = new LinkedHashMap<>();
    private final ArrayDeque<History> pendingHistory = new ArrayDeque<>();
    private final Map<String, Long> historyAttempts = new LinkedHashMap<>();
    private final Set<String> activePipelines = new HashSet<>();
    private final Map<String, Integer> waitingBarriers = new HashMap<>();
    private final Map<String, Integer> activeHistory = new HashMap<>();
    private final Map<String, TerminalHistory> terminals = new HashMap<>();
    private final java.util.function.LongSupplier nanoTime;
    private long historySequence;
    private final AtomicBoolean drainingHistory = new AtomicBoolean();
    private final ScheduledThreadPoolExecutor observationTimer;
    private final ExecutorService observationWorkers;
    private final ScheduledThreadPoolExecutor historyTimer;
    private long retainedBytes;
    private int inFlight;
    private long retainedHistoryBytes;
    private int retainedHistoryEntries;
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
        observationWorkers = Executors.newThreadPerTaskExecutor(task -> ShutdownAwait.virtual(task, "pipeline-observation-batch"));
        historyTimer = timer("pipeline-history-writer");
        if (scheduled) {
            long delay = interval.toNanos();
            observationTimer.scheduleWithFixedDelay(this::dispatchObservations, delay, delay, TimeUnit.NANOSECONDS);
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

    private final class TerminalHistory implements TerminalPublication.Receipt {
        final TerminalPublication publication;
        boolean appended, failed, sealed;
        TerminalHistory(TerminalPublication publication) { this.publication = publication; }
        public void reopen() {
            synchronized (pending) { if (terminals.get(publication.scope().pipelineId()) == this) sealed = false; }
        }
        public boolean seal() {
            synchronized (pending) {
                if (status() != TerminalPublication.Status.COMPLETE) return false;
                sealed = true;return true;
            }
        }
        public TerminalPublication.Status status() {
            synchronized (pending) {
                String id = publication.scope().pipelineId();
                if (closed || failed || terminals.get(id) != this) return TerminalPublication.Status.FAILED;
                if (!appended || activePipelines.contains(id) || pending.containsKey(id)
                        || activeHistory.containsKey(id) || pendingHistory.stream().anyMatch(sample -> sample.pipeline().equals(id)))
                    return TerminalPublication.Status.PENDING;
                return TerminalPublication.Status.COMPLETE;
            }
        }
    }

    void expecting(PipelineConvergencePass pass) {
        synchronized (pending) { terminals.remove(pass.scope().pipelineId()); }
    }

    /** The terminal append returns only after storage acknowledges it; ordinary sampler enqueueing is not a receipt. */
    TerminalPublication.Receipt terminalHistory(TerminalPublication publication, History sample) {
        String id = publication.scope().pipelineId();
        synchronized (pending) {
            if (closed || sample != null && (!id.equals(sample.pipeline()) || !publication.observedAt().equals(sample.observedAt())))
                return TerminalPublication.failed();
            var previous = terminals.get(id);
            if (previous != null && previous.publication.equals(publication)
                    && previous.status() != TerminalPublication.Status.FAILED) return previous;
            if (previous == null && terminals.size() >= MAX_ENTRIES) return TerminalPublication.failed();
            var receipt = new TerminalHistory(publication);
            terminals.put(id, receipt);
            if (sample == null) receipt.appended = true;
            else {
                var tracked = new History(sample.pipeline(), sample.observedAt(), sample.bytes(), sample.requireActive(), remaining -> {
                    sample.append().accept(remaining);
                    synchronized (pending) { receipt.appended = true; }
                });
                if (pending.size() + pendingHistory.size() + inFlight >= MAX_ENTRIES
                        || retainedBytes + tracked.bytes() > MAX_BYTES || !enqueueHistory(tracked)) receipt.failed = true;
            }
            return receipt;
        }
    }

    private void failTerminal(String pipeline) {
        synchronized (pending) { var terminal = terminals.get(pipeline); if (terminal != null && !terminal.sealed) terminal.failed = true; }
    }

    boolean submit(Stage stage, Runnable committed, Runnable failed) {
        return submit(stage, committed, null, failed);
    }

    boolean submit(Stage stage, Runnable committed, History sampled, Runnable failed) {
        if (stage.observation == null) return true;
        boolean sealed;
        synchronized (pending) { var terminal = terminals.get(stage.pipeline());sealed = terminal != null && terminal.sealed; }
        if (sealed) { failed.run();return false; }
        failTerminal(stage.pipeline());
        stage.committed.add(committed);
        stage.sampled = sampled == null ? null : sampled.guardedBy(stage.original);
        stage.failed = failed;
        stage.bytes = estimatedBytes(stage.observation) + (sampled == null ? 0 : sampled.bytes());
        boolean accepted;
        synchronized (pending) {
            Stage previous = pending.get(stage.pipeline());
            long nextBytes = retainedBytes - (previous == null ? 0 : previous.bytes) + stage.bytes;
            var terminal = terminals.get(stage.pipeline());
            accepted = !closed && (terminal == null || !terminal.sealed) && nextBytes <= MAX_BYTES
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
            var terminal = terminals.get(sample.pipeline());
            if (terminal != null && terminal.sealed) return;
            failTerminal(sample.pipeline());
            if (closed) return;
            if (pending.size() + pendingHistory.size() + inFlight < MAX_ENTRIES
                    && retainedBytes + sample.bytes() <= MAX_BYTES && enqueueHistory(sample)) {
                return;
            }
        }
        LOG.warn("History write capacity is exhausted for pipeline {}", sample.pipeline());
    }

    /** Lifecycle barriers use the same entrance and finish before their checkpoint transition proceeds. */
    void now(String pipeline, Runnable publication) {
        reserveBarrier(pipeline);
        try {
            inPipeline(pipeline, publication);
        } finally {
            synchronized (pending) {
                activePipelines.remove(pipeline);
                pending.notifyAll();
            }
        }
    }

    private void reserveBarrier(String pipeline) {
        long deadline = nanoTime.getAsLong() + BARRIER_WAIT_NANOS;
        synchronized (pending) {
            waitingBarriers.merge(pipeline, 1, Integer::sum);
            try {
                while (!closed && activePipelines.contains(pipeline)) {
                    long remaining = deadline - nanoTime.getAsLong();
                    if (remaining <= 0) throw StorageRuntime.unavailable();
                    TimeUnit.NANOSECONDS.timedWait(pending, remaining);
                }
                if (closed || nanoTime.getAsLong() - deadline >= 0) throw StorageRuntime.unavailable();
                activePipelines.add(pipeline);
                Stage removed = pending.remove(pipeline);
                if (removed != null) { retainedBytes -= removed.bytes; failTerminal(pipeline); }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw StorageRuntime.unavailable();
            } finally {
                waitingBarriers.computeIfPresent(pipeline, (id, count) -> count == 1 ? null : count - 1);
            }
        }
    }

    void retain(Collection<String> pipelines) {
        synchronized (pending) {
            var iterator = pending.entrySet().iterator();
            while (iterator.hasNext()) {
                Stage stage = iterator.next().getValue();
                if (!pipelines.contains(stage.pipeline())) {
                    retainedBytes -= stage.bytes;
                    failTerminal(stage.pipeline());
                    iterator.remove();
                }
            }
            var history = pendingHistory.iterator();
            while (history.hasNext()) {
                History sample = history.next();
                if (!pipelines.contains(sample.pipeline())) {
                    failTerminal(sample.pipeline());
                    releaseHistory(sample);
                    history.remove();
                }
            }
            terminals.keySet().retainAll(pipelines);
            var attempts = historyAttempts.keySet().iterator();
            while (attempts.hasNext()) {
                String pipeline = attempts.next();
                if (!pipelines.contains(pipeline)) {
                    accountHistoryBytes(-historyBytes(pipeline));
                    attempts.remove();
                }
            }
        }
    }

    void flush() {
        CompletableFuture<Void> batch;
        synchronized (pending) { batch = dispatchBatch(); }
        if (batch != null) batch.join();
        drainHistory();
    }

    private void dispatchObservations() {
        synchronized (pending) {
            // With admission locked, a tick can dispatch only the entries it already owns.
            while (!pending.isEmpty()) {
                if (dispatchBatch() == null) return;
            }
        }
    }

    /** Called under the admission lock. Every dispatched task holds the entries and bytes of its batch. */
    private CompletableFuture<Void> dispatchBatch() {
        if (closed) return null;
        List<Stage> batch = new ArrayList<>();
        long bytes = 0;
        var iterator = pending.entrySet().iterator();
        while (iterator.hasNext() && batch.size() < BATCH_ENTRIES) {
            var entry = iterator.next();
            String pipeline = entry.getKey();
            if (activePipelines.contains(pipeline) || waitingBarriers.containsKey(pipeline)) continue;
            Stage next = entry.getValue();
            if (!batch.isEmpty() && bytes + next.bytes > BATCH_BYTES) break;
            activePipelines.add(pipeline);
            batch.add(next);
            bytes += next.bytes;
            iterator.remove();
        }
        if (batch.isEmpty()) return null;
        inFlight += batch.size();
        return CompletableFuture.runAsync(() -> writeBatch(batch), observationWorkers);
    }

    private void writeBatch(List<Stage> batch) {
        List<History> history = new ArrayList<>();
        try {
            writeObservations(batch, history);
        } catch (Error fatal) {
            batch.forEach(stage -> failTerminal(stage.pipeline()));throw fatal;
        } finally {
            handoffHistory(batch, history);
        }
    }

    private void handoffHistory(List<Stage> batch, List<History> history) {
        int dropped = 0;
        synchronized (pending) {
            for (Stage stage : batch) {
                retainedBytes -= stage.bytes;
                activePipelines.remove(stage.pipeline());
            }
            inFlight -= batch.size();
            pending.notifyAll();
            if (closed) return;
            // Each completed Stage already reserved one entry and the bytes of its compact sample.
            for (History sample : history) {
                if (!enqueueHistory(sample)) { failTerminal(sample.pipeline()); dropped++; }
            }
        }
        if (dropped > 0) LOG.warn("History write capacity is exhausted; dropped {} committed observation samples", dropped);
    }

    private boolean enqueueHistory(History sample) {
        if (retainedHistoryEntries >= MAX_HISTORY_ENTRIES
                || retainedHistoryBytes + sample.bytes() > MAX_HISTORY_BYTES) return false;
        pendingHistory.addLast(sample);
        retainedHistoryEntries++;
        accountHistoryBytes(sample.bytes());
        return true;
    }

    private void releaseHistory(History sample) {
        retainedHistoryEntries--;
        accountHistoryBytes(-sample.bytes());
    }

    private void accountHistoryBytes(long delta) {
        retainedHistoryBytes += delta;
        retainedBytes += delta;
    }

    private void writeObservations(List<Stage> batch, List<History> history) {
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
                        try {
                            stage.active.run();
                        } catch (RuntimeException guardFailure) {
                            failTerminal(stage.pipeline());
                            stage.failed.run();
                            throw guardFailure;
                        }
                        stage.committed.forEach(Runnable::run);
                        if (stage.sampled != null) history.add(stage.sampled);
                    } else {
                        LOG.warn("Could not publish the observation of pipeline {}", stage.pipeline(), failure);
                        failTerminal(stage.pipeline());
                        stage.failed.run();
                    }
                });
            } catch (RuntimeException failure) {
                failTerminal(stage.pipeline());
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
        var appended = new AtomicBoolean();
        try {
            if (rememberHistoryAttempt(sample.pipeline())) inPipeline(sample.pipeline(), () -> {
                sample.requireActive().run();
                long remaining = deadline - nanoTime.getAsLong();
                if (remaining > 0) { sample.append().accept(Duration.ofNanos(remaining)); appended.set(true); }
            });
        } catch (RuntimeException failure) {
            LOG.warn("Could not sample the history of pipeline {}", sample.pipeline(), failure);
        } finally {
            synchronized (pending) {
                if (!appended.get()) failTerminal(sample.pipeline());
                releaseHistory(sample);
                inFlight--;
                activeHistory.computeIfPresent(sample.pipeline(), (id, count) -> count == 1 ? null : count - 1);
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
                activeHistory.merge(first.pipeline(), 1, Integer::sum);
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
            if (historyAttempts.remove(pipeline) != null) accountHistoryBytes(-historyBytes(pipeline));
            long bytes = historyBytes(pipeline);
            while (!historyAttempts.isEmpty() && (historyAttempts.size() >= MAX_ENTRIES || !canRetainHistoryBytes(bytes))) {
                String oldest = historyAttempts.keySet().iterator().next();
                historyAttempts.remove(oldest);
                accountHistoryBytes(-historyBytes(oldest));
            }
            if (closed || !canRetainHistoryBytes(bytes)) return false;
            historyAttempts.put(pipeline, ++historySequence);
            accountHistoryBytes(bytes);
            return true;
        }
    }

    private boolean canRetainHistoryBytes(long bytes) {
        return retainedBytes + bytes <= MAX_BYTES && retainedHistoryBytes + bytes <= MAX_HISTORY_BYTES;
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
            for (History sample : pendingHistory) releaseHistory(sample);
            pendingHistory.clear();
            for (String pipeline : historyAttempts.keySet()) accountHistoryBytes(-historyBytes(pipeline));
            historyAttempts.clear();
            pending.notifyAll();
        }
        observationTimer.shutdown();
        observationWorkers.shutdown();
        historyTimer.shutdown();
        long deadline = ShutdownAwait.deadline();
        ShutdownAwait.executor(observationTimer, deadline);
        ShutdownAwait.executor(observationWorkers, deadline);
        ShutdownAwait.executor(historyTimer, deadline);
    }

    final class Stage {
        private final PipelineConvergencePass pass;
        private final WorkloadClaimFence claim;
        private final Runnable original;
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
            this.original = pass.guardOriginal(claim);
            this.active = original;
        }

        String pipeline() { return pass.scope().pipelineId(); }

        MongoCacheDomain.ObservationWrite write() {
            return new MongoCacheDomain.ObservationWrite(pass.scope(), claim, active, observation, epoch);
        }

        ObservationStore wrap(ObservationStore destination) {
            Runnable originalGuard = original;
            Runnable measured = destination instanceof ActiveStore guarded ? guarded.writeGuard() : () -> {};
            return new ObservationStore() {
                @Override public void save(Observation value) { throw new IllegalArgumentException("an observation requires its measured checkpoint epoch"); }
                @Override public void save(Observation value, long checkpointEpoch) {
                    observation = value;
                    epoch = checkpointEpoch;
                    active = () -> { originalGuard.run(); measured.run(); };
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
