package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.Clock;
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
import java.util.function.Consumer;

/** Fixed worker budgets keep slow telemetry stores away from convergence and data-plane calls. */
final class TelemetryDispatcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryDispatcher.class);
    static final int DEFAULT_LATEST_WORKERS = 4;
    static final int DEFAULT_QUEUE_CAPACITY = 64;
    private static final Duration DEFAULT_WRITE_DEADLINE = Duration.ofSeconds(5);
    private static final Duration CLOSE_DEADLINE = Duration.ofSeconds(2);
    private static final Duration BREAKER_COOLDOWN = Duration.ofSeconds(1);
    private record FailureTime(Instant occurredAt, long nanos) { }

    enum Sink {
        LATEST, HISTORY, EXPORT, EVENT
    }

    enum BreakerState {
        CLOSED(0), OPEN(1), HALF_OPEN(2);

        private final long value;

        BreakerState(long value) {
            this.value = value;
        }

        long value() {
            return value;
        }
    }

    record Health(int queueDepth, long highWater, int inFlight, long coalesced, long dropped,
            long successes, long failures, long timeouts, long maxDurationMillis,
            OptionalLong lastSuccessAgeMillis,
            boolean degraded, int openGaps, int pendingRestorations, long gapsOpened, long gapsClosed,
            BreakerState breakerState, long breakerRecoveries, Instant gapsStartedAt) {

        private Health withGaps(int open, int pending, long opened, long closed, Instant countingSince) {
            return new Health(queueDepth, highWater, inFlight, coalesced, dropped, successes,
                    failures, timeouts, maxDurationMillis, lastSuccessAgeMillis,
                    degraded || open > 0 || pending > 0, open, pending, opened, closed,
                    breakerState, breakerRecoveries, Objects.requireNonNull(countingSince, "countingSince"));
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
            private final String pipelineId;
            private volatile ObservationStore.Scope scope;
            private volatile ObservationScopeRegistry.RestoreTicket restoreTicket;
            private volatile FailureTime failureTime;
            /** 0 running, 1 timed out while running, 2 finished. */
            private final AtomicInteger state = new AtomicInteger();

            private Operation(String pipelineId, ObservationStore.Scope scope) {
                this.pipelineId = pipelineId;
                this.scope = scope;
            }
        }

        private final AtomicLong highWater = new AtomicLong();
        private final AtomicLong coalesced = new AtomicLong();
        private final AtomicLong dropped = new AtomicLong();
        private final AtomicLong successes = new AtomicLong();
        private final AtomicLong failures = new AtomicLong();
        private final AtomicLong timeouts = new AtomicLong();
        private final AtomicLong breakerRecoveries = new AtomicLong();
        private final AtomicLong maxDurationNanos = new AtomicLong();
        private final AtomicInteger consecutiveFailures = new AtomicInteger();
        private final AtomicLong openUntilNanos = new AtomicLong();
        private final AtomicBoolean probe = new AtomicBoolean();
        private final ConcurrentHashMap<Thread, Operation> inFlight = new ConcurrentHashMap<>();
        private final AtomicLong lastSuccessStartedNanos = new AtomicLong();
        private final AtomicLong lastProblemNanos = new AtomicLong();
        private volatile long lastSuccessNanos;

        private static long laterTimestamp(long previous, long candidate) {
            return previous == 0 || candidate - previous > 0 ? candidate : previous;
        }

        private void queueDepth(int depth) {
            highWater.accumulateAndGet(depth, Math::max);
        }

        private void dropped() {
            dropped.incrementAndGet();
            lastProblemNanos.accumulateAndGet(System.nanoTime(), Stats::laterTimestamp);
        }

        private void dropped(long count) {
            if (count > 0) {
                dropped.addAndGet(count);
                lastProblemNanos.accumulateAndGet(System.nanoTime(), Stats::laterTimestamp);
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
            lastProblemNanos.accumulateAndGet(now, Stats::laterTimestamp);
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
            return begin(null, null);
        }

        private Operation begin(String pipelineId, ObservationStore.Scope scope) {
            Operation operation = new Operation(pipelineId, scope);
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

        private boolean completed(Operation operation, boolean success) {
            boolean timedOut = operation.state.getAndSet(2) == 1;
            boolean recordedSuccess = success && !timedOut;
            inFlight.remove(Thread.currentThread(), operation);
            maxDurationNanos.accumulateAndGet(System.nanoTime() - operation.started, Math::max);
            if (recordedSuccess) {
                successes.incrementAndGet();
                lastSuccessNanos = System.nanoTime();
                lastSuccessStartedNanos.accumulateAndGet(operation.started, Stats::laterTimestamp);
                consecutiveFailures.set(0);
                if (openUntilNanos.getAndSet(0) != 0) {
                    breakerRecoveries.incrementAndGet();
                }
                probe.set(false);
            } else {
                failures.incrementAndGet();
                lastProblemNanos.accumulateAndGet(System.nanoTime(), Stats::laterTimestamp);
                if (timedOut || probe.get() || consecutiveFailures.incrementAndGet() >= 3) {
                    openUntilNanos.set(System.nanoTime() + BREAKER_COOLDOWN.toNanos());
                    probe.set(false);
                }
            }
            return recordedSuccess;
        }

        private void watch(String sink, Duration deadline, Consumer<Operation> onTimeout) {
            long now = System.nanoTime();
            for (Operation operation : inFlight.values()) {
                if (now - operation.started >= deadline.toNanos() && operation.state.get() == 0) {
                    operation.failureTime = new FailureTime(Instant.now(), now);
                    if (!operation.state.compareAndSet(0, 1)) {
                        operation.failureTime = null;
                        continue;
                    }
                    timeouts.incrementAndGet();
                    lastProblemNanos.accumulateAndGet(now, Stats::laterTimestamp);
                    openUntilNanos.set(now + BREAKER_COOLDOWN.toNanos());
                    probe.set(false);
                    if (onTimeout != null) {
                        onTimeout.accept(operation);
                    }
                    LOG.warn("{} telemetry write exceeded its {} ms deadline", sink, deadline.toMillis());
                }
            }
        }

        private Health snapshot(int queueDepth) {
            long successfulAt = lastSuccessNanos;
            long successfulStart = lastSuccessStartedNanos.get();
            long problemAt = lastProblemNanos.get();
            boolean timedOutInFlight = inFlight.values().stream().anyMatch(op -> op.state.get() == 1);
            BreakerState breakerState = openUntilNanos.get() == 0 ? BreakerState.CLOSED
                    : probe.get() ? BreakerState.HALF_OPEN : BreakerState.OPEN;
            return new Health(queueDepth, highWater.get(), inFlight.size(), coalesced.get(), dropped.get(),
                    successes.get(), failures.get(), timeouts.get(),
                    TimeUnit.NANOSECONDS.toMillis(maxDurationNanos.get()),
                    successfulAt == 0 ? OptionalLong.empty()
                            : OptionalLong.of(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - successfulAt)),
                    timedOutInFlight || (problemAt != 0
                            && (successfulStart == 0 || problemAt - successfulStart > 0)), 0, 0, 0, 0,
                    breakerState, breakerRecoveries.get(), null);
        }
    }

    private sealed interface Frame permits ObservationFrame, ReconcileFailureFrame, RecoveryFrame {
        ObservationStore.Scope scope();
    }

    private record ObservationFrame(ObservationPublisher.Prepared prepared, ObservationStore.Scope scope)
            implements Frame {
    }

    private record ReconcileFailureFrame(String pipelineId, long failures, ObservationStore.Scope scope)
            implements Frame {
    }

    private record RecoveryFrame(ObservationScopeRegistry.RestoreTicket ticket, BooleanSupplier owner)
            implements Frame {
        @Override public ObservationStore.Scope scope() { return null; }
    }

    private final ObservationPublisher publisher;
    private final RateSampler sampler;
    private final MetricsExport export;
    private final ObservationScopeRegistry scopes;
    private final ObservationScopeRecovery scopeRecovery;
    private final PipelineEventStore events;
    private final TelemetryBoundaryEvents boundaryEvents;
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
        this(publisher, sampler, export, scopes, events, null, latestConcurrency, queueCapacity, writeDeadline);
    }

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            ObservationScopeRegistry scopes, PipelineEventStore events, ObservationScopeRecovery scopeRecovery,
            int latestConcurrency, int queueCapacity) {
        this(publisher, sampler, export, scopes, events, scopeRecovery, latestConcurrency, queueCapacity,
                DEFAULT_WRITE_DEADLINE);
    }

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            ObservationScopeRegistry scopes, PipelineEventStore events, ObservationScopeRecovery scopeRecovery,
            int latestConcurrency, int queueCapacity, Duration writeDeadline) {
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.sampler = sampler;
        this.export = Objects.requireNonNull(export, "export");
        this.scopes = scopes;
        this.scopeRecovery = scopeRecovery;
        if (scopes != null) {
            export.bindCurrentScopes(id -> scopes.current(id).map(owner -> new MetricsExport.ScopeToken(
                    owner.pipelineIncarnationId(), owner.executionGeneration())));
        }
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
        boundaryEvents = events == null ? null : new TelemetryBoundaryEvents(eventGapCapacity,
                Clock.systemUTC(), this::stillCurrent, this::enqueueEvent,
                event -> lostEvent(event, PipelineEvent.GapReason.QUEUE_FULL));
        watchdog = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "tapstate-telemetry-watchdog");
            thread.setDaemon(true);
            return thread;
        });
        long periodMillis = Math.max(10, Math.min(1000, writeDeadline.toMillis() / 4));
        watchdog.scheduleWithFixedDelay(() -> {
            latestStats.watch("latest", writeDeadline, this::latestFailure);
            historyStats.watch("history", writeDeadline, op -> boundaryFailed(op.pipelineId, op.scope, Sink.HISTORY));
            exportStats.watch("export", writeDeadline, op -> boundaryFailed(op.pipelineId, op.scope, Sink.EXPORT));
            if (eventWorker != null) {
                eventStats.watch("event", writeDeadline, null);
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
        enqueueEvent(event);
    }

    private void enqueueEvent(PipelineEvent event) {
        if (eventWorker == null) {
            return;
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
        offerProjections(frame, scope);
    }

    private void offerProjections(ObservationPublisher.Prepared frame, ObservationStore.Scope scope) {
        Observation observation = frame.observation();
        if (sampler != null) {
            offerSide(historyWorker, historyStats, observation.pipelineId(), Sink.HISTORY, scope,
                    () -> stillCurrent(observation.pipelineId(), scope)
                            && sampler.appendIfDue(observation, scope),
                    () -> {
                        if (stillCurrent(observation.pipelineId(), scope)) {
                            sampler.markDropped(observation, scope);
                        }
                    });
        }
        if (export != MetricsExport.none()) {
            offerSide(exportWorker, exportStats, observation.pipelineId(), Sink.EXPORT, scope, () -> {
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
                if (scope == null) {
                    export.offerFolded(observation.pipelineId(), observation.state(),
                            observation.observedAt(), observation.facts());
                } else {
                    export.offerFoldedScoped(observation.pipelineId(), new MetricsExport.ScopeToken(
                            scope.pipelineIncarnationId(), scope.executionGeneration()), observation.state(),
                            observation.observedAt(), observation.facts());
                }
                if (scope != null && scopes != null) {
                    offeredScopes.put(observation.pipelineId(), scope);
                }
                return true;
            }, null);
        }
    }

    private boolean stillCurrent(String pipelineId, ObservationStore.Scope scope) {
        return scopes == null || (scope != null && scopes.current(pipelineId).filter(scope::equals).isPresent());
    }

    private void boundaryFailed(String id, ObservationStore.Scope scope, Sink sink) {
        if (boundaryEvents != null) {
            boundaryEvents.failed(id, scope, sink);
        }
    }

    private void rememberColdFailure(Stats.Operation operation) {
        if (operation.restoreTicket != null && scopes != null) {
            FailureTime failed = operation.state.get() == 1 && operation.failureTime != null
                    ? operation.failureTime : new FailureTime(Instant.now(), System.nanoTime());
            scopes.rememberRestorationFailure(operation.restoreTicket, operation.scope,
                    failed.occurredAt(), failed.nanos());
        }
    }

    private void latestFailure(Stats.Operation operation) {
        rememberColdFailure(operation);
        if (operation.restoreTicket != null && scopes != null && boundaryEvents != null) {
            var retained = scopes.restorationTelemetryFailure(operation.restoreTicket, operation.scope)
                    .orElse(null);
            FailureTime failed = retained != null
                    ? new FailureTime(retained.occurredAt(), retained.lastFailureNanos())
                    : operation.failureTime != null ? operation.failureTime
                            : new FailureTime(Instant.now(), System.nanoTime());
            boundaryEvents.failed(operation.pipelineId, operation.scope, Sink.LATEST,
                    failed.occurredAt(), failed.nanos());
            return;
        }
        boundaryFailed(operation.pipelineId, operation.scope, Sink.LATEST);
    }

    private void successfulCompletion(Stats stats, Stats.Operation operation, Sink sink) {
        if (stats.completed(operation, true)) {
            if (boundaryEvents != null && (sink != Sink.HISTORY
                    || !sampler.hasOpenGap(operation.pipelineId, operation.scope))) {
                boundaryEvents.succeeded(operation.pipelineId, operation.scope, sink, operation.started);
            }
        } else {
            boundaryFailed(operation.pipelineId, operation.scope, sink);
        }
    }

    Map<Sink, Health> health() {
        Map<Sink, Health> readings = new java.util.EnumMap<>(Sink.class);
        readings.put(Sink.LATEST, latestStats.snapshot(latestWorkers.getQueue().size() + latestPending.get()));
        Health history = historyStats.snapshot(historyWorker.getQueue().size());
        if (sampler != null) {
            RateSampler.GapHealth gap = sampler.gapHealth();
            history = history.withGaps(gap.open(), 0, gap.opened(), gap.closed(), gap.startedAt());
        }
        readings.put(Sink.HISTORY, history);
        readings.put(Sink.EXPORT, exportStats.snapshot(exportWorker.getQueue().size()));
        if (eventWorker != null) {
            readings.put(Sink.EVENT, eventStats.snapshot(eventWorker.getQueue().size())
                    .withGaps(openEventGaps(), pendingEventRestorations(),
                            gapsOpened.get(), gapsClosed.get(), startedAt));
        }
        return Map.copyOf(readings);
    }

    void offerReconcileFailure(String pipelineId, long failures, ObservationStore.Scope scope) {
        if (closed.get()) {
            return;
        }
        offerLatest(pipelineId, new ReconcileFailureFrame(pipelineId, failures, scope));
    }

    /** Identity and latest reads share the same bounded, coalescing cold lane as latest writes. */
    void offerScopeRecovery(String pipelineId, ConvergeResult result, ObservationFailure failure,
            BooleanSupplier owner) {
        offerScopeRecovery(pipelineId, result, failure, null, owner);
    }

    void offerScopeRecovery(String pipelineId, ConvergeResult result, ObservationFailure failure,
            ObservationScopeRecovery.Owner ownerVersion, BooleanSupplier owner) {
        if (closed.get() || scopes == null || scopeRecovery == null) {
            return;
        }
        scopes.restoration(pipelineId, result, failure, ownerVersion).ifPresent(ticket ->
                offerLatest(pipelineId, new RecoveryFrame(ticket, Objects.requireNonNull(owner, "owner"))));
    }

    private boolean recover(String pipelineId, RecoveryFrame frame, Stats.Operation operation) {
        var ticket = frame.ticket();
        if (!scopes.awaiting(ticket) || !frame.owner().getAsBoolean()) {
            return false;
        }
        var qualified = scopeRecovery.resolve(pipelineId).orElse(null);
        if (qualified == null || !scopes.awaiting(ticket) || !frame.owner().getAsBoolean()
                || !scopeRecovery.matchesOwner(pipelineId, qualified, ticket.owner())
                || !scopeRecovery.unchanged(pipelineId, qualified)) {
            return false;
        }
        operation.scope = qualified.scope();
        var prepared = scopes.restorationPrepared(ticket, qualified).orElse(null);
        if (prepared == null) {
            var attempt = scopes.restorationFailure(ticket, qualified.checkpoint());
            prepared = publisher.prepareScoped(pipelineId,
                    attempt != null && attempt.first() ? attempt.failure() : null, qualified.scope()).orElse(null);
            // Preparation can fail before or after its local failure account is updated. A neutral
            // retry reads that account; only a still unrecorded cause is handed back for counting.
            if (prepared != null && attempt != null && !attempt.first()
                    && prepared.observation().state() == StateJson.parse(qualified.checkpoint().stateJson())
                    && !recordedFailure(prepared, attempt.failure())) {
                prepared = publisher.prepareScoped(pipelineId, attempt.failure(), qualified.scope()).orElse(null);
            }
            if (prepared == null || !scopes.rememberRestoration(ticket, qualified, prepared)) {
                return false;
            }
        }
        if (prepared == null || prepared.observation().state() != StateJson.parse(qualified.checkpoint().stateJson())
                || !scopes.awaiting(ticket) || !frame.owner().getAsBoolean()
                || !scopeRecovery.unchanged(pipelineId, qualified)) {
            return false;
        }
        ObservationStore.Scope scope = qualified.scope();
        var persisted = publisher.commit(scopes.restorationFrame(ticket, qualified.stored(), prepared), scope);
        if (persisted.isEmpty()) {
            scopes.retryRestoration(ticket);
            return false;
        }
        if (operation.state.get() == 1) {
            rememberColdFailure(operation);
        }
        // No local scope exists during a slow/failed write. A late callback cannot open publication
        // after its owner, authority, checkpoint or registry ticket has changed.
        if (!scopes.awaiting(ticket) || !frame.owner().getAsBoolean()
                || !scopeRecovery.unchanged(pipelineId, qualified)
                || !scopes.restore(ticket, qualified.stored())) {
            return false;
        }
        // A historical transition retains its own checkpoint version, independently of latest state.
        for (var signal : scopes.restorationSignals(ticket)) {
            PipelineStateEvents.of(pipelineId, scope, signal.result(), signal.failure()).forEach(this::offerEvent);
        }
        if (boundaryEvents != null) {
            scopes.restorationTelemetryFailure(ticket, scope).ifPresent(failed -> boundaryEvents.failed(
                    pipelineId, scope, Sink.LATEST, failed.occurredAt(), failed.lastFailureNanos()));
        }
        scopes.restored(ticket);
        offerProjections(scopes.continueFrame(prepared, scope), scope);
        return true;
    }

    private static boolean recordedFailure(ObservationPublisher.Prepared prepared, ObservationFailure failure) {
        return prepared.observation().facts().stream()
                .filter(fact -> "tapstate.pipeline.errors".equals(fact.name()))
                .flatMap(fact -> fact.points().stream())
                .anyMatch(point -> failure.code().equals(point.attributes().get(MetricAttributes.CODE))
                        && point.value() > 0);
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
                boundaryFailed(pipelineId, frame.scope(), Sink.LATEST);
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
                boundaryFailed(pipelineId, frame.scope(), Sink.LATEST);
                LOG.warn("Latest observation for pipeline {} was dropped: telemetry workers stopped", pipelineId);
            }
            return;
        }
    }

    private void offerSide(ThreadPoolExecutor workers, Stats stats, String pipelineId, Sink sink,
            ObservationStore.Scope scope,
            BooleanSupplier write, Runnable onDrop) {
        try {
            workers.execute(() -> {
                if (!stats.allow()) {
                    stats.dropped();
                    dropped(onDrop, sink.name().toLowerCase(java.util.Locale.ROOT), pipelineId);
                    boundaryFailed(pipelineId, scope, sink);
                    return;
                }
                Stats.Operation operation = stats.begin(pipelineId, scope);
                try {
                    if (write.getAsBoolean()) {
                        successfulCompletion(stats, operation, sink);
                    } else {
                        stats.skipped(operation);
                    }
                } catch (RuntimeException failed) {
                    stats.completed(operation, false);
                    boundaryFailed(pipelineId, scope, sink);
                    LOG.warn("Could not write {} telemetry for pipeline {}",
                            sink.name().toLowerCase(java.util.Locale.ROOT), pipelineId, failed);
                } catch (Error defect) {
                    stats.completed(operation, false);
                    throw defect;
                }
            });
            stats.queueDepth(workers.getQueue().size());
        } catch (java.util.concurrent.RejectedExecutionException saturated) {
            stats.dropped();
            dropped(onDrop, sink.name().toLowerCase(java.util.Locale.ROOT), pipelineId);
            boundaryFailed(pipelineId, scope, sink);
            LOG.warn("{} telemetry for pipeline {} was dropped: queue is full",
                    sink.name().toLowerCase(java.util.Locale.ROOT), pipelineId);
        }
    }

    private static void dropped(Runnable onDrop, String sink, String pipelineId) {
        if (onDrop == null) {
            return;
        }
        try {
            onDrop.run();
        } catch (RuntimeException failed) {
            LOG.warn("Could not record dropped {} telemetry for pipeline {}", sink, pipelineId, failed);
        }
    }

    void retain(Collection<String> pipelineIds) {
        offeredScopes.keySet().retainAll(pipelineIds);
        if (boundaryEvents != null) {
            boundaryEvents.retain(pipelineIds);
        }
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
                    boundaryFailed(pipelineId, frame.scope(), Sink.LATEST);
                    continue;
                }
                Stats.Operation operation = latestStats.begin(pipelineId, frame.scope());
                if (frame instanceof RecoveryFrame recovery) {
                    operation.restoreTicket = recovery.ticket();
                }
                try {
                    if (frame instanceof ObservationFrame observation) {
                        if (publisher.commit(observation.prepared(), observation.scope()).isPresent()) {
                            successfulCompletion(latestStats, operation, Sink.LATEST);
                        } else {
                            latestStats.skipped(operation);
                        }
                    } else if (frame instanceof ReconcileFailureFrame failure) {
                        if (publisher.commitReconcileFailure(failure.pipelineId(), failure.failures(),
                                failure.scope())) {
                            successfulCompletion(latestStats, operation, Sink.LATEST);
                        } else {
                            latestStats.skipped(operation);
                        }
                    } else if (frame instanceof RecoveryFrame recovery) {
                        if (recover(pipelineId, recovery, operation)) {
                            successfulCompletion(latestStats, operation, Sink.LATEST);
                        } else {
                            latestStats.skipped(operation);
                        }
                    }
                } catch (RuntimeException failed) {
                    latestFailure(operation);
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
