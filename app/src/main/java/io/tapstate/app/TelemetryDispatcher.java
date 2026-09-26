package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.OptionalLong;
import java.util.Map;
import java.util.function.BooleanSupplier;

/** Fixed worker budgets keep slow telemetry stores away from convergence and data-plane calls. */
final class TelemetryDispatcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryDispatcher.class);
    static final int DEFAULT_LATEST_WORKERS = 4;
    static final int DEFAULT_QUEUE_CAPACITY = 64;
    private static final Duration DEFAULT_WRITE_DEADLINE = Duration.ofSeconds(5);
    private static final Duration CLOSE_DEADLINE = Duration.ofSeconds(2);
    private static final Duration BREAKER_COOLDOWN = Duration.ofSeconds(1);

    enum Sink {
        LATEST, HISTORY, EXPORT, EVENT
    }

    record Health(int queueDepth, long highWater, int inFlight, long coalesced, long dropped,
            long successes, long failures, long timeouts, long maxDurationMillis,
            OptionalLong lastSuccessAgeMillis,
            boolean degraded, int openGaps, int pendingRestorations, long gapsOpened, long gapsClosed) {

        private Health withGaps(int open, int pending, long opened, long closed) {
            return new Health(queueDepth, highWater, inFlight, coalesced, dropped, successes,
                    failures, timeouts, maxDurationMillis, lastSuccessAgeMillis,
                    degraded || open > 0 || pending > 0, open, pending, opened, closed);
        }
    }

    private record EventKey(String pipelineId, String incarnationId, long executionGeneration) {
        private static EventKey of(PipelineEvent event) {
            return new EventKey(event.pipelineId(), event.pipelineIncarnationId(), event.executionGeneration());
        }
    }

    private static final class OpenGap {
        private final Instant from;
        private Instant to;
        private final EnumSet<PipelineEvent.GapReason> reasons;
        private long version;

        private OpenGap(Instant at, PipelineEvent.GapReason reason) {
            from = at;
            to = at;
            reasons = EnumSet.of(reason);
        }

        private void include(Instant at, PipelineEvent.GapReason reason) {
            if (at.isAfter(to)) {
                to = at;
            }
            reasons.add(reason);
            version++;
        }

        private GapAttempt attempt(EventKey key) {
            PipelineEvent.Gap gap = new PipelineEvent.Gap(from, to, List.copyOf(reasons));
            PipelineEvent marker = new PipelineEvent(
                    PipelineEvent.gapId(key.pipelineId(), key.incarnationId(),
                            key.executionGeneration(), from),
                    key.pipelineId(), key.incarnationId(), key.executionGeneration(),
                    PipelineEvent.Kind.TELEMETRY_GAP, from, null, null, null, null, gap);
            return new GapAttempt(this, version, marker);
        }
    }

    private record GapAttempt(OpenGap owner, long version, PipelineEvent marker) {
    }

    private static final class Stats {
        private static final class Operation {
            private final long started = System.nanoTime();
            /** 0 running, 1 timed out while running, 2 finished. */
            private final AtomicInteger state = new AtomicInteger();
        }

        private final AtomicLong highWater = new AtomicLong();
        private final AtomicLong coalesced = new AtomicLong();
        private final AtomicLong dropped = new AtomicLong();
        private final AtomicLong successes = new AtomicLong();
        private final AtomicLong failures = new AtomicLong();
        private final AtomicLong timeouts = new AtomicLong();
        private final AtomicLong maxDurationNanos = new AtomicLong();
        private final AtomicInteger consecutiveFailures = new AtomicInteger();
        private final AtomicLong openUntilNanos = new AtomicLong();
        private final AtomicBoolean probe = new AtomicBoolean();
        private final ConcurrentHashMap<Thread, Operation> inFlight = new ConcurrentHashMap<>();
        private volatile long lastSuccessNanos;
        private volatile long lastProblemNanos;

        private void queueDepth(int depth) {
            highWater.accumulateAndGet(depth, Math::max);
        }

        private void dropped() {
            dropped.incrementAndGet();
            lastProblemNanos = System.nanoTime();
        }

        private void dropped(long count) {
            if (count > 0) {
                dropped.addAndGet(count);
                lastProblemNanos = System.nanoTime();
            }
        }

        private void flushTimedOut() {
            long now = System.nanoTime();
            long newlyTimedOut = 0;
            for (Operation operation : inFlight.values()) {
                if (operation.state.compareAndSet(0, 1)) {
                    newlyTimedOut++;
                }
            }
            timeouts.addAndGet(newlyTimedOut);
            lastProblemNanos = now;
        }

        private boolean allow() {
            long until = openUntilNanos.get();
            if (until == 0) {
                return true;
            }
            if (System.nanoTime() - until < 0) {
                return false;
            }
            return probe.compareAndSet(false, true);
        }

        private Operation begin() {
            Operation operation = new Operation();
            inFlight.put(Thread.currentThread(), operation);
            return operation;
        }

        private void skipped(Operation operation) {
            operation.state.set(2);
            inFlight.remove(Thread.currentThread(), operation);
            maxDurationNanos.accumulateAndGet(System.nanoTime() - operation.started, Math::max);
            if (probe.getAndSet(false)) {
                openUntilNanos.set(System.nanoTime() + BREAKER_COOLDOWN.toNanos());
            }
        }

        private void completed(Operation operation, boolean success) {
            boolean timedOut = operation.state.getAndSet(2) == 1;
            inFlight.remove(Thread.currentThread(), operation);
            maxDurationNanos.accumulateAndGet(System.nanoTime() - operation.started, Math::max);
            if (success && !timedOut) {
                successes.incrementAndGet();
                lastSuccessNanos = System.nanoTime();
                consecutiveFailures.set(0);
                openUntilNanos.set(0);
                probe.set(false);
            } else {
                failures.incrementAndGet();
                lastProblemNanos = System.nanoTime();
                if (timedOut || probe.get() || consecutiveFailures.incrementAndGet() >= 3) {
                    openUntilNanos.set(System.nanoTime() + BREAKER_COOLDOWN.toNanos());
                    probe.set(false);
                }
            }
        }

        private void watch(String sink, Duration deadline) {
            long now = System.nanoTime();
            for (Operation operation : inFlight.values()) {
                if (now - operation.started >= deadline.toNanos()
                        && operation.state.compareAndSet(0, 1)) {
                    timeouts.incrementAndGet();
                    lastProblemNanos = now;
                    openUntilNanos.set(now + BREAKER_COOLDOWN.toNanos());
                    probe.set(false);
                    LOG.warn("{} telemetry write exceeded its {} ms deadline", sink, deadline.toMillis());
                }
            }
        }

        private Health snapshot(int queueDepth) {
            long successfulAt = lastSuccessNanos;
            boolean timedOutInFlight = inFlight.values().stream().anyMatch(op -> op.state.get() == 1);
            return new Health(queueDepth, highWater.get(), inFlight.size(), coalesced.get(), dropped.get(),
                    successes.get(), failures.get(), timeouts.get(),
                    TimeUnit.NANOSECONDS.toMillis(maxDurationNanos.get()),
                    successfulAt == 0 ? OptionalLong.empty()
                            : OptionalLong.of(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - successfulAt)),
                    timedOutInFlight || (lastProblemNanos != 0
                            && (successfulAt == 0 || lastProblemNanos - successfulAt > 0)), 0, 0, 0, 0);
        }
    }

    private sealed interface Frame permits ObservationFrame, ReconcileFailureFrame {
    }

    private record ObservationFrame(ObservationPublisher.Prepared prepared, ObservationStore.Scope scope)
            implements Frame {
    }

    private record ReconcileFailureFrame(String pipelineId, long failures, ObservationStore.Scope scope)
            implements Frame {
    }

    private final ObservationPublisher publisher;
    private final RateSampler sampler;
    private final MetricsExport export;
    private final ObservationScopeRegistry scopes;
    private final PipelineEventStore events;
    private final ThreadPoolExecutor latestWorkers;
    private final ThreadPoolExecutor historyWorker;
    private final ThreadPoolExecutor exportWorker;
    private final ThreadPoolExecutor eventWorker;
    private final Semaphore latestCapacity;
    private final AtomicInteger latestPending = new AtomicInteger();
    private final Stats latestStats = new Stats();
    private final Stats historyStats = new Stats();
    private final Stats exportStats = new Stats();
    private final Stats eventStats = new Stats();
    private final ConcurrentHashMap<String, LatestSlot> latestByPipeline = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ObservationStore.Scope> offeredScopes = new ConcurrentHashMap<>();
    private final Object eventGapLock = new Object();
    private final LinkedHashMap<EventKey, OpenGap> eventGaps = new LinkedHashMap<>();
    private final LinkedHashMap<EventKey, PipelineEvent> pendingRestorations = new LinkedHashMap<>();
    private final LinkedHashMap<EventKey, Instant> lastGapFrom = new LinkedHashMap<>(16, 0.75f, true);
    private final ConcurrentHashMap<Thread, EventTask> eventsInFlight = new ConcurrentHashMap<>();
    private final AtomicLong gapsOpened = new AtomicLong();
    private final AtomicLong gapsClosed = new AtomicLong();
    private final AtomicLong recoveryCursor = new AtomicLong();
    private final AtomicBoolean recoveryScheduled = new AtomicBoolean();
    private final int eventGapCapacity;
    private final ScheduledExecutorService watchdog;
    private final Instant startedAt = Instant.now();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean abort = new AtomicBoolean();

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            int latestConcurrency, int queueCapacity) {
        this(publisher, sampler, export, null, latestConcurrency, queueCapacity);
    }

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            ObservationScopeRegistry scopes, int latestConcurrency, int queueCapacity) {
        this(publisher, sampler, export, scopes, latestConcurrency, queueCapacity, DEFAULT_WRITE_DEADLINE);
    }

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            ObservationScopeRegistry scopes, PipelineEventStore events,
            int latestConcurrency, int queueCapacity) {
        this(publisher, sampler, export, scopes, events, latestConcurrency, queueCapacity,
                DEFAULT_WRITE_DEADLINE);
    }

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            ObservationScopeRegistry scopes, int latestConcurrency, int queueCapacity, Duration writeDeadline) {
        this(publisher, sampler, export, scopes, null, latestConcurrency, queueCapacity, writeDeadline);
    }

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            ObservationScopeRegistry scopes, PipelineEventStore events,
            int latestConcurrency, int queueCapacity, Duration writeDeadline) {
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.sampler = sampler;
        this.export = Objects.requireNonNull(export, "export");
        this.scopes = scopes;
        this.events = events;
        if (latestConcurrency < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("telemetry worker and queue budgets must be positive");
        }
        if (writeDeadline.isZero() || writeDeadline.isNegative()) {
            throw new IllegalArgumentException("telemetry write deadline must be positive");
        }
        latestCapacity = new Semaphore(Math.addExact(latestConcurrency, queueCapacity));
        eventGapCapacity = Math.addExact(latestConcurrency, queueCapacity);
        latestWorkers = workers("latest", latestConcurrency, queueCapacity);
        historyWorker = workers("history", 1, queueCapacity);
        exportWorker = workers("export", 1, queueCapacity);
        eventWorker = events == null ? null : workers("event", 1, queueCapacity);
        watchdog = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "tapstate-telemetry-watchdog");
            thread.setDaemon(true);
            return thread;
        });
        long periodMillis = Math.max(10, Math.min(1000, writeDeadline.toMillis() / 4));
        watchdog.scheduleWithFixedDelay(() -> {
            latestStats.watch("latest", writeDeadline);
            historyStats.watch("history", writeDeadline);
            exportStats.watch("export", writeDeadline);
            if (eventWorker != null) {
                eventStats.watch("event", writeDeadline);
                retryOpenGap();
            }
        }, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
        EnumSet<Sink> wired = EnumSet.of(Sink.LATEST);
        if (sampler != null) {
            wired.add(Sink.HISTORY);
        }
        if (export != MetricsExport.none()) {
            wired.add(Sink.EXPORT);
        }
        if (events != null) {
            wired.add(Sink.EVENT);
        }
        Set<Sink> enabled = Set.copyOf(wired);
        export.observeProcess("telemetry",
                () -> TelemetryProcessFacts.snapshot(health(), enabled, startedAt, Instant.now()));
    }

    void offerEvent(PipelineEvent event) {
        Objects.requireNonNull(event, "event");
        if (eventWorker == null) {
            return;
        }
        if (event.kind() == PipelineEvent.Kind.TELEMETRY_GAP
                || event.kind() == PipelineEvent.Kind.TELEMETRY_RESTORED) {
            throw new IllegalArgumentException("event gap recovery is owned by the dispatcher");
        }
        if (closed.get()) {
            lostEvent(event, PipelineEvent.GapReason.SHUTDOWN);
            return;
        }
        try {
            eventWorker.execute(new EventTask(event));
            eventStats.queueDepth(eventWorker.getQueue().size());
        } catch (java.util.concurrent.RejectedExecutionException saturated) {
            lostEvent(event, closed.get() ? PipelineEvent.GapReason.SHUTDOWN
                    : PipelineEvent.GapReason.QUEUE_FULL);
        }
    }

    int openEventGaps() {
        synchronized (eventGapLock) {
            return eventGaps.size();
        }
    }

    int pendingEventRestorations() {
        synchronized (eventGapLock) {
            return pendingRestorations.size();
        }
    }

    private void lostEvent(PipelineEvent event, PipelineEvent.GapReason reason) {
        eventStats.dropped();
        EventKey key = EventKey.of(event);
        // Loss is observed at offer/write time; older queued events can fail after newer rejected ones.
        Instant lostAt = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        synchronized (eventGapLock) {
            OpenGap existing = eventGaps.get(key);
            if (existing != null) {
                existing.include(lostAt, reason);
            } else if (eventGaps.size() + pendingRestorations.size() < eventGapCapacity) {
                Instant previous = lastGapFrom.get(key);
                if (previous != null && !lostAt.isAfter(previous)) {
                    lostAt = previous.plusMillis(1);
                }
                lastGapFrom.put(key, lostAt);
                if (lastGapFrom.size() > eventGapCapacity) {
                    lastGapFrom.remove(lastGapFrom.keySet().iterator().next());
                }
                eventGaps.put(key, new OpenGap(lostAt, reason));
                gapsOpened.incrementAndGet();
            } else {
                LOG.warn("Event telemetry loss for pipeline {} exceeded the {} open-gap budget",
                        event.pipelineId(), eventGapCapacity);
            }
        }
        LOG.warn("Event telemetry for pipeline {} was dropped: {}", event.pipelineId(), reason);
    }

    private boolean recoverEventState(EventKey key) {
        return persistRestoration(key) && recoverGap(key);
    }

    private boolean recoverGap(EventKey key) {
        GapAttempt attempt;
        synchronized (eventGapLock) {
            OpenGap open = eventGaps.get(key);
            if (open == null) {
                return true;
            }
            attempt = open.attempt(key);
        }
        if (!eventStats.allow()) {
            return false;
        }
        Stats.Operation operation = eventStats.begin();
        try {
            events.append(attempt.marker());
            eventStats.completed(operation, true);
        } catch (RuntimeException failed) {
            eventStats.completed(operation, false);
            LOG.warn("Could not persist event gap marker for pipeline {}", key.pipelineId(), failed);
            return false;
        } catch (Error defect) {
            eventStats.completed(operation, false);
            throw defect;
        }

        boolean closedGap = false;
        synchronized (eventGapLock) {
            OpenGap current = eventGaps.get(key);
            if (current == attempt.owner() && current.version == attempt.version()) {
                eventGaps.remove(key);
                gapsClosed.incrementAndGet();
                pendingRestorations.put(key, new PipelineEvent(attempt.marker().id() + "-restored",
                        key.pipelineId(), key.incarnationId(), key.executionGeneration(),
                        PipelineEvent.Kind.TELEMETRY_RESTORED, Instant.now(),
                        null, null, null, null, null));
                closedGap = true;
            }
        }
        return closedGap && persistRestoration(key);
    }

    private boolean persistRestoration(EventKey key) {
        PipelineEvent restored;
        synchronized (eventGapLock) {
            restored = pendingRestorations.get(key);
        }
        if (restored == null) {
            return true;
        }
        if (!eventStats.allow()) {
            return false;
        }
        Stats.Operation operation = eventStats.begin();
        try {
            events.append(restored);
            eventStats.completed(operation, true);
            synchronized (eventGapLock) {
                pendingRestorations.remove(key, restored);
            }
            return true;
        } catch (RuntimeException failed) {
            eventStats.completed(operation, false);
            // A failed restoration stays pending; recursively opening a gap would never converge.
            eventStats.dropped();
            LOG.warn("Could not persist event telemetry restoration for pipeline {}",
                    key.pipelineId(), failed);
            return false;
        } catch (Error defect) {
            eventStats.completed(operation, false);
            throw defect;
        }
    }

    private void retryOpenGap() {
        if (closed.get() || !recoveryScheduled.compareAndSet(false, true)) {
            return;
        }
        EventKey key;
        synchronized (eventGapLock) {
            if (eventGaps.isEmpty() && pendingRestorations.isEmpty()) {
                recoveryScheduled.set(false);
                return;
            }
            java.util.LinkedHashSet<EventKey> outstanding = new java.util.LinkedHashSet<>(pendingRestorations.keySet());
            outstanding.addAll(eventGaps.keySet());
            List<EventKey> keys = List.copyOf(outstanding);
            key = keys.get(Math.floorMod(recoveryCursor.getAndIncrement(), keys.size()));
        }
        try {
            eventWorker.execute(() -> {
                try {
                    recoverEventState(key);
                } finally {
                    recoveryScheduled.set(false);
                }
            });
            eventStats.queueDepth(eventWorker.getQueue().size());
        } catch (java.util.concurrent.RejectedExecutionException saturated) {
            recoveryScheduled.set(false);
        }
    }

    private final class EventTask implements Runnable {
        private final PipelineEvent event;
        private final AtomicBoolean lost = new AtomicBoolean();

        private EventTask(PipelineEvent event) {
            this.event = event;
        }

        private void cancel(PipelineEvent.GapReason reason) {
            if (lost.compareAndSet(false, true)) {
                lostEvent(event, reason);
            }
        }

        @Override
        public void run() {
            if (abort.get()) {
                cancel(PipelineEvent.GapReason.SHUTDOWN);
                return;
            }
            if (!recoverEventState(EventKey.of(event)) || !eventStats.allow()) {
                cancel(PipelineEvent.GapReason.WRITE_FAILURE);
                return;
            }
            Stats.Operation operation = eventStats.begin();
            eventsInFlight.put(Thread.currentThread(), this);
            try {
                events.append(event);
                eventStats.completed(operation, true);
            } catch (RuntimeException failed) {
                eventStats.completed(operation, false);
                cancel(PipelineEvent.GapReason.WRITE_FAILURE);
                LOG.warn("Could not persist event telemetry for pipeline {}", event.pipelineId(), failed);
            } catch (Error defect) {
                eventStats.completed(operation, false);
                throw defect;
            } finally {
                eventsInFlight.remove(Thread.currentThread(), this);
            }
        }
    }

    private static ThreadPoolExecutor workers(String sink, int count, int queueCapacity) {
        AtomicInteger next = new AtomicInteger();
        return new ThreadPoolExecutor(count, count, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), task -> {
                    Thread thread = new Thread(task, "tapstate-telemetry-" + sink + "-" + next.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    void offer(ObservationPublisher.Prepared prepared, ObservationStore.Scope scope) {
        Objects.requireNonNull(prepared, "prepared");
        if (closed.get()) {
            return;
        }
        if (scopes != null && (scope == null || scopes.current(prepared.observation().pipelineId())
                .filter(scope::equals).isEmpty())) {
            return;
        }
        ObservationPublisher.Prepared frame = scopes == null ? prepared : scopes.continueFrame(prepared, scope);
        Observation observation = frame.observation();
        offerLatest(observation.pipelineId(), new ObservationFrame(frame, scope));
        if (sampler != null) {
            offerSide(historyWorker, historyStats, observation.pipelineId(), "history",
                    () -> stillCurrent(observation.pipelineId(), scope)
                            && sampler.appendIfDue(observation, scope));
        }
        if (export != MetricsExport.none()) {
            offerSide(exportWorker, exportStats, observation.pipelineId(), "export", () -> {
                if (!stillCurrent(observation.pipelineId(), scope)) {
                    return false;
                }
                if (scope != null && scopes != null) {
                    ObservationStore.Scope prior = offeredScopes.get(observation.pipelineId());
                    if ((prior == null || !prior.equals(scope))
                            && !scopes.continuing(observation.pipelineId(), scope)) {
                        export.forgetPipeline(observation.pipelineId());
                    }
                }
                export.offer(observation.pipelineId(), observation.state(),
                        observation.observedAt(), observation.facts());
                if (scope != null && scopes != null) {
                    offeredScopes.put(observation.pipelineId(), scope);
                }
                return true;
            });
        }
    }

    private boolean stillCurrent(String pipelineId, ObservationStore.Scope scope) {
        return scopes == null || (scope != null && scopes.current(pipelineId).filter(scope::equals).isPresent());
    }

    Map<Sink, Health> health() {
        Map<Sink, Health> readings = new java.util.EnumMap<>(Sink.class);
        readings.put(Sink.LATEST, latestStats.snapshot(latestWorkers.getQueue().size() + latestPending.get()));
        readings.put(Sink.HISTORY, historyStats.snapshot(historyWorker.getQueue().size()));
        readings.put(Sink.EXPORT, exportStats.snapshot(exportWorker.getQueue().size()));
        if (eventWorker != null) {
            readings.put(Sink.EVENT, eventStats.snapshot(eventWorker.getQueue().size())
                    .withGaps(openEventGaps(), pendingEventRestorations(),
                            gapsOpened.get(), gapsClosed.get()));
        }
        return Map.copyOf(readings);
    }

    void offerReconcileFailure(String pipelineId, long failures, ObservationStore.Scope scope) {
        if (closed.get()) {
            return;
        }
        offerLatest(pipelineId, new ReconcileFailureFrame(pipelineId, failures, scope));
    }

    private void offerLatest(String pipelineId, Frame frame) {
        while (true) {
            LatestSlot existing = latestByPipeline.get(pipelineId);
            if (existing != null) {
                synchronized (existing) {
                    if (existing.retired) {
                        continue;
                    }
                    if (existing.pending != null) {
                        latestStats.coalesced.incrementAndGet();
                    } else if (existing.started) {
                        existing.pendingCounted = true;
                        latestPending.incrementAndGet();
                    }
                    existing.pending = frame;
                    latestStats.queueDepth(latestWorkers.getQueue().size() + latestPending.get());
                    return;
                }
            }
            if (!latestCapacity.tryAcquire()) {
                latestStats.dropped();
                LOG.warn("Latest observation for pipeline {} was dropped: telemetry queue is full", pipelineId);
                return;
            }
            LatestSlot created = new LatestSlot(pipelineId, frame);
            if (latestByPipeline.putIfAbsent(pipelineId, created) != null) {
                latestCapacity.release();
                continue;
            }
            try {
                latestWorkers.execute(created);
                latestStats.queueDepth(latestWorkers.getQueue().size() + latestPending.get());
            } catch (java.util.concurrent.RejectedExecutionException unavailable) {
                synchronized (created) {
                    created.retired = true;
                    created.pending = null;
                    latestByPipeline.remove(pipelineId, created);
                }
                latestCapacity.release();
                latestStats.dropped();
                LOG.warn("Latest observation for pipeline {} was dropped: telemetry workers stopped", pipelineId);
            }
            return;
        }
    }

    private static void offerSide(ThreadPoolExecutor workers, Stats stats, String pipelineId, String sink,
            BooleanSupplier write) {
        try {
            workers.execute(() -> {
                if (!stats.allow()) {
                    stats.dropped();
                    return;
                }
                Stats.Operation operation = stats.begin();
                try {
                    if (write.getAsBoolean()) {
                        stats.completed(operation, true);
                    } else {
                        stats.skipped(operation);
                    }
                } catch (RuntimeException failed) {
                    stats.completed(operation, false);
                    LOG.warn("Could not write {} telemetry for pipeline {}", sink, pipelineId, failed);
                } catch (Error defect) {
                    stats.completed(operation, false);
                    throw defect;
                }
            });
            stats.queueDepth(workers.getQueue().size());
        } catch (java.util.concurrent.RejectedExecutionException saturated) {
            stats.dropped();
            LOG.warn("{} telemetry for pipeline {} was dropped: queue is full", sink, pipelineId);
        }
    }

    void retain(Collection<String> pipelineIds) {
        offeredScopes.keySet().retainAll(pipelineIds);
        for (String id : latestByPipeline.keySet()) {
            if (!pipelineIds.contains(id)) {
                LatestSlot slot = latestByPipeline.get(id);
                if (slot != null) {
                    synchronized (slot) {
                        if (slot.pendingCounted) {
                            latestPending.decrementAndGet();
                            slot.pendingCounted = false;
                        }
                        slot.pending = null;
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        watchdog.shutdownNow();
        latestWorkers.shutdown();
        historyWorker.shutdown();
        exportWorker.shutdown();
        if (eventWorker != null) {
            eventWorker.shutdown();
        }
        long deadline = System.nanoTime() + CLOSE_DEADLINE.toNanos();
        drain(latestWorkers, latestStats, "latest", deadline);
        drain(historyWorker, historyStats, "history", deadline);
        drain(exportWorker, exportStats, "export", deadline);
        if (eventWorker != null) {
            drain(eventWorker, eventStats, "event", deadline);
            synchronized (eventGapLock) {
                if (!eventGaps.isEmpty()) {
                    for (OpenGap gap : eventGaps.values()) {
                        gap.include(gap.to, PipelineEvent.GapReason.SHUTDOWN);
                    }
                    LOG.warn("Event telemetry closed with {} unpersisted gap intervals", eventGaps.size());
                }
                if (!pendingRestorations.isEmpty()) {
                    LOG.warn("Event telemetry closed with {} unpersisted restoration events",
                            pendingRestorations.size());
                }
            }
        }
    }

    private void drain(ThreadPoolExecutor workers, Stats stats, String sink, long deadline) {
        boolean complete = false;
        try {
            complete = workers.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        if (!complete) {
            abort.set(true);
            stats.flushTimedOut();
            List<Runnable> abandoned = workers.shutdownNow();
            if (workers != eventWorker) {
                stats.dropped(abandoned.size());
            }
            abandoned.stream().filter(LatestSlot.class::isInstance).map(LatestSlot.class::cast)
                    .forEach(LatestSlot::cancel);
            if (workers == eventWorker) {
                abandoned.stream().filter(EventTask.class::isInstance).map(EventTask.class::cast)
                        .forEach(task -> task.cancel(PipelineEvent.GapReason.SHUTDOWN));
                eventsInFlight.values().forEach(task -> task.cancel(PipelineEvent.GapReason.SHUTDOWN));
            }
            LOG.warn("{} telemetry flush exceeded {} ms: inFlight={}, queued={}",
                    sink, CLOSE_DEADLINE.toMillis(), stats.inFlight.size(), abandoned.size());
        }
    }

    private final class LatestSlot implements Runnable {
        private final String pipelineId;
        private Frame pending;
        private boolean retired;
        private boolean started;
        private boolean pendingCounted;

        private LatestSlot(String pipelineId, Frame first) {
            this.pipelineId = pipelineId;
            this.pending = first;
        }

        private synchronized void cancel() {
            retired = true;
            pending = null;
            latestByPipeline.remove(pipelineId, this);
            latestCapacity.release();
        }

        @Override
        public void run() {
            while (true) {
                Frame frame;
                synchronized (this) {
                    started = true;
                    if (pendingCounted) {
                        latestPending.decrementAndGet();
                        pendingCounted = false;
                    }
                    boolean aborting = abort.get();
                    if (aborting && pending != null) {
                        latestStats.dropped();
                    }
                    frame = aborting ? null : pending;
                    pending = null;
                    if (frame == null) {
                        retired = true;
                        latestByPipeline.remove(pipelineId, this);
                        latestCapacity.release();
                        return;
                    }
                }
                if (!latestStats.allow()) {
                    latestStats.dropped();
                    continue;
                }
                Stats.Operation operation = latestStats.begin();
                try {
                    if (frame instanceof ObservationFrame observation) {
                        if (publisher.commit(observation.prepared(), observation.scope()).isPresent()) {
                            latestStats.completed(operation, true);
                        } else {
                            latestStats.skipped(operation);
                        }
                    } else if (frame instanceof ReconcileFailureFrame failure) {
                        if (failure.scope() == null) {
                            publisher.publishReconcileFailure(failure.pipelineId(), failure.failures());
                        } else {
                            publisher.publishReconcileFailureScoped(
                                    failure.pipelineId(), failure.failures(), failure.scope());
                        }
                        latestStats.completed(operation, true);
                    }
                } catch (RuntimeException failed) {
                    latestStats.completed(operation, false);
                    LOG.warn("Could not write latest observation for pipeline {}", pipelineId, failed);
                } catch (Error defect) {
                    latestStats.completed(operation, false);
                    throw defect;
                }
            }
        }
    }
}
