package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.CheckpointDoc;
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
import org.slf4j.MDC;

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
import java.util.function.Supplier;
import java.util.function.Consumer;

/** Fixed worker budgets keep telemetry collection and stores away from convergence and data-plane calls. */
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

    enum PublicationQualification {
        CURRENT, RETRY, STALE
    }

    /** One diagnostic retained only by its bounded preparation or recovery request. */
    static final class FailureLog {
        private final String pipelineId;
        private final ObservationFailure failure;
        private final Throwable cause;
        private final AtomicBoolean emitted = new AtomicBoolean();

        FailureLog(String pipelineId, ObservationFailure failure, Throwable cause) {
            this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId");
            this.failure = Objects.requireNonNull(failure, "failure");
            this.cause = cause;
        }

        void emit(ObservationStore.Scope scope, BooleanSupplier current) {
            if (scope == null || emitted.get() || !current.getAsBoolean()
                    || !emitted.compareAndSet(false, true)) { return; }
            PipelineLogContext previous = PipelineLogContext.capture();
            MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, pipelineId);
            PipelineLogContext.bindScope(scope);
            try {
                ConvergenceDriver.logFailure(pipelineId, failure, cause);
            } finally {
                previous.restore();
            }
        }
    }

    private enum PreparationOutcome {
        PUBLISHED, RETRY, SKIPPED
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
            private final PreparationFrame preparation;
            /** Set synchronously only after the publisher registered this request's non-null cause. */
            private boolean failureCaptured;
            private volatile FailureTime failureTime;
            /** 0 running, 1 timed out while running, 2 finished. */
            private final AtomicInteger state = new AtomicInteger();

            private Operation(String pipelineId, ObservationStore.Scope scope, PreparationFrame preparation) {
                this.pipelineId = pipelineId;
                this.scope = scope;
                this.preparation = preparation;
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
            return begin(pipelineId, scope, null);
        }

        private Operation begin(String pipelineId, ObservationStore.Scope scope, PreparationFrame preparation) {
            Operation operation = new Operation(pipelineId, scope, preparation);
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

    private sealed interface Frame permits ObservationFrame, PreparationFrame, ReconcileFailureFrame, RecoveryFrame {
        ObservationStore.Scope scope();
    }

    private record ObservationFrame(ObservationPublisher.Prepared prepared, ObservationStore.Scope scope)
            implements Frame {
    }

    private record RecoveryQualification(CheckpointDoc checkpoint, ObservationScopeRecovery.Owner ownerVersion,
            BooleanSupplier owner) { }

    /** Captures publication authority and the one observed cause without collecting native metrics. */
    private record PreparationFrame(String pipelineId, ObservationFailure failure, ObservationStore.Scope scope,
            Supplier<PublicationQualification> owner, Instant requestedAt, FailureLog diagnostic,
            RecoveryQualification recovery) implements Frame {
        private PreparationFrame(String pipelineId, ObservationFailure failure, ObservationStore.Scope scope,
                Supplier<PublicationQualification> owner, Instant requestedAt, FailureLog diagnostic) {
            this(pipelineId, failure, scope, owner, requestedAt, diagnostic, null);
        }

        private PreparationFrame {
            Objects.requireNonNull(pipelineId, "pipelineId");
            Objects.requireNonNull(owner, "owner");
            Objects.requireNonNull(requestedAt, "requestedAt");
        }

        private PreparationFrame replacing(PreparationFrame previous) {
            return previous.failure() != null && Objects.equals(scope, previous.scope())
                    ? new PreparationFrame(pipelineId, previous.failure(), scope, owner, requestedAt,
                            previous.diagnostic(), previous.recovery()) : this;
        }
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
    private final ObservationContinuationRecovery continuations;
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
        this(publisher, sampler, export, scopes, events, scopeRecovery, null,
                latestConcurrency, queueCapacity, writeDeadline);
    }

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            ObservationScopeRegistry scopes, PipelineEventStore events, ObservationScopeRecovery scopeRecovery,
            ObservationContinuationRecovery continuations, int latestConcurrency, int queueCapacity,
            Duration writeDeadline) {
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.sampler = sampler;
        this.export = Objects.requireNonNull(export, "export");
        this.scopes = scopes;
        this.scopeRecovery = scopeRecovery;
        this.continuations = continuations;
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

    /** The scheduler offers only immutable inputs; collection and folding share the latest worker budget. */
    void offerPreparation(String pipelineId, ObservationFailure failure, ObservationStore.Scope scope,
            BooleanSupplier owner) {
        Objects.requireNonNull(owner, "owner");
        offerQualifiedPreparation(pipelineId, failure, scope,
                () -> owner.getAsBoolean() ? PublicationQualification.CURRENT : PublicationQualification.STALE);
    }

    void offerQualifiedPreparation(String pipelineId, ObservationFailure failure, ObservationStore.Scope scope,
            Supplier<PublicationQualification> owner) {
        offerQualifiedPreparation(pipelineId, failure, scope, owner, null);
    }

    void offerQualifiedPreparation(String pipelineId, ObservationFailure failure, ObservationStore.Scope scope,
            Supplier<PublicationQualification> owner, FailureLog diagnostic) {
        PreparationFrame request = new PreparationFrame(pipelineId, failure, scope, owner, Instant.now(), diagnostic);
        if (closed.get()) {
            return;
        }
        if (qualification(request) == PublicationQualification.STALE) {
            discardStalePreparation(pipelineId);
            return;
        }
        offerLatest(pipelineId, request);
    }

    private PublicationQualification qualification(PreparationFrame request) {
        return abort.get() || !stillCurrent(request.pipelineId(), request.scope())
                ? PublicationQualification.STALE : Objects.requireNonNull(request.owner().get(), "qualification");
    }

    private boolean eligible(PreparationFrame request) {
        return qualification(request) == PublicationQualification.CURRENT;
    }

    private PreparationOutcome prepareAndCommit(PreparationFrame request, Stats.Operation operation) {
        PipelineLogContext previous = PipelineLogContext.capture();
        MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, request.pipelineId());
        PipelineLogContext.bindScope(request.scope());
        try {
            return prepareAndCommitOwned(request, operation);
        } finally {
            previous.restore();
        }
    }

    /** Cold fallback reads occur only on the existing latest worker, after the request was admitted. */
    private BooleanSupplier preparationCurrent(PreparationFrame request) {
        if (request.recovery() == null) { return () -> eligible(request); }
        RecoveryQualification expected = request.recovery();
        var qualified = scopeRecovery.resolve(request.pipelineId()).orElse(null);
        if (qualified == null || !qualified.scope().equals(request.scope())
                || !qualified.checkpoint().equals(expected.checkpoint())
                || !scopeRecovery.matchesOwner(request.pipelineId(), qualified, expected.ownerVersion())
                || !expected.owner().getAsBoolean()
                || !stillCurrent(request.pipelineId(), qualified.scope())
                || !scopeRecovery.unchanged(request.pipelineId(), qualified)) { return null; }
        return () -> eligible(request) && expected.owner().getAsBoolean()
                && stillCurrent(request.pipelineId(), qualified.scope())
                && scopeRecovery.unchanged(request.pipelineId(), qualified);
    }

    private PreparationOutcome prepareAndCommitOwned(PreparationFrame request, Stats.Operation operation) {
        PublicationQualification before = qualification(request);
        if (before != PublicationQualification.CURRENT) {
            if (before == PublicationQualification.STALE) {
                discardStalePreparation(request.pipelineId());
            }
            return before == PublicationQualification.RETRY ? PreparationOutcome.RETRY : PreparationOutcome.SKIPPED;
        }
        BooleanSupplier current = preparationCurrent(request);
        if (current == null) { return PreparationOutcome.SKIPPED; }
        if (continuations != null && !continuations.prepareHandoff(request.pipelineId(), request.scope(), current)) {
            return eligible(request) ? PreparationOutcome.RETRY : PreparationOutcome.SKIPPED;
        }
        Runnable captured = () -> {
            operation.failureCaptured = true;
            if (request.diagnostic() != null) { request.diagnostic().emit(request.scope(), current); }
        };
        var prepared = request.scope() == null
                ? publisher.prepare(request.pipelineId(), request.failure(), current, captured)
                : publisher.prepareScoped(request.pipelineId(), request.failure(), request.scope(), current, captured);
        if (prepared.isEmpty()) {
            // The publisher acknowledges its actual account update, independently of guard success.
            // Retrying an already captured non-null cause would count the same death twice.
            return !operation.failureCaptured && qualification(request) == PublicationQualification.RETRY
                    ? PreparationOutcome.RETRY : PreparationOutcome.SKIPPED;
        }
        if (!eligible(request)) {
            return PreparationOutcome.SKIPPED;
        }
        var packet = continuationPacket(prepared.orElseThrow(), request.scope(), current);
        if (continuations != null && scopes.activeContinuationKey(request.pipelineId()).isPresent() && packet.isEmpty()) {
            return PreparationOutcome.SKIPPED;
        }
        ObservationPublisher.Prepared frame = packet.map(ObservationScopeRegistry.ContinuationPublication::projected)
                .orElseGet(() -> scopes == null ? prepared.orElseThrow()
                        : scopes.continueFrame(prepared.orElseThrow(), request.scope()));
        if (!eligible(request)) {
            return PreparationOutcome.SKIPPED;
        }
        var published = commitContinuation(frame, request.scope(), current, packet);
        if (published.isEmpty() || !eligible(request)) {
            return PreparationOutcome.SKIPPED;
        }
        // commit may carry a stored failure; all successful projections still use these same measured facts.
        if (published.orElseThrow() != frame.observation()) {
            frame = new ObservationPublisher.Prepared(published.orElseThrow(), false,
                    frame.nestReadings(), frame.pinned(), frame.gaps());
        }
        offerProjections(frame, request.scope());
        return PreparationOutcome.PUBLISHED;
    }

    private java.util.Optional<ObservationScopeRegistry.ContinuationPublication> continuationPacket(
            ObservationPublisher.Prepared prepared, ObservationStore.Scope scope, BooleanSupplier current) {
        return continuations == null || scope == null ? java.util.Optional.empty()
                : continuations.measuredTarget(prepared.observation().pipelineId(), scope, current)
                        .flatMap(target -> scopes.prepareContinuationPublication(prepared, target, current));
    }

    private java.util.Optional<Observation> commitContinuation(ObservationPublisher.Prepared prepared,
            ObservationStore.Scope scope, BooleanSupplier current,
            java.util.Optional<ObservationScopeRegistry.ContinuationPublication> packet) {
        if (packet.isEmpty() || packet.orElseThrow().snapshot().isEmpty()) {
            return publisher.commit(prepared, scope, current);
        }
        var bound = packet.orElseThrow();
        return publisher.commit(prepared, scope, current, (observation, owner) -> {
            var result = continuations.publish(observation, owner,
                    ObservationStore.ContinuationWrite.store(bound.snapshot().orElseThrow(), bound.expectedReceipt()));
            if (result.committed()) {
                result.continuationReceipt().ifPresent(receipt -> scopes.publicationAccepted(bound.ticket(), receipt));
            } else if (current.getAsBoolean()) {
                // A racing accepted writer changed the private revision. Read once on this cold refusal path.
                continuations.resolveExisting(observation.pipelineId(), current).ifPresent(continuations::adoptExisting);
            }
            return result.committed();
        });
    }

    void discardStalePreparations() {
        latestByPipeline.keySet().forEach(this::discardStalePreparation);
    }

    void discardStalePreparation(String pipelineId) {
        LatestSlot slot = latestByPipeline.get(pipelineId);
        if (slot == null) {
            return;
        }
        Frame pending;
        synchronized (slot) {
            pending = slot.pending;
        }
        // The external predicate stays outside the monitor. Cleanup matches this exact input snapshot.
        if (!(pending instanceof PreparationFrame preparation)
                || qualification(preparation) != PublicationQualification.STALE) {
            return;
        }
        synchronized (slot) {
            if (latestByPipeline.get(pipelineId) != slot || slot.pending != pending) {
                return;
            }
            slot.pending = null;
            if (slot.pendingCounted) {
                latestPending.decrementAndGet();
                slot.pendingCounted = false;
            }
            if (!slot.scheduled) {
                slot.retire();
            }
        }
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
        if (operation.preparation != null) {
            preparationDropped(operation.preparation);
        }
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

    private void preparationDropped(PreparationFrame request) {
        if (sampler != null && stillCurrent(request.pipelineId(), request.scope())) {
            sampler.markPreparationDropped(request.pipelineId(), request.scope(), request.requestedAt());
        }
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
        offerScopeRecovery(pipelineId, result, failure, ownerVersion, owner, null);
    }

    void offerScopeRecovery(String pipelineId, ConvergeResult result, ObservationFailure failure,
            ObservationScopeRecovery.Owner ownerVersion, BooleanSupplier owner, FailureLog diagnostic) {
        if (closed.get() || scopes == null || scopeRecovery == null) {
            return;
        }
        Objects.requireNonNull(owner, "owner");
        var ticket = scopes.restoration(pipelineId, result, failure, ownerVersion, diagnostic);
        if (ticket.isPresent()) {
            offerLatest(pipelineId, new RecoveryFrame(ticket.orElseThrow(), owner));
        } else if (result != null && failure != null
                && result.status() == io.tapstate.runtime.scheduler.ConvergeStatus.FAILED
                && result.checkpoint().isPresent()) {
            // A prior recovery may have registered a scope after the scheduler read it as absent.
            // This local scope is only a candidate; the worker qualifies the original checkpoint and authority.
            scopes.current(pipelineId).ifPresent(candidate -> offerLatest(pipelineId,
                    new PreparationFrame(pipelineId, failure, candidate,
                            () -> owner.getAsBoolean() ? PublicationQualification.CURRENT : PublicationQualification.STALE,
                            Instant.now(), diagnostic,
                            new RecoveryQualification(result.checkpoint().orElseThrow(), ownerVersion, owner))));
        }
    }

    private boolean recover(String pipelineId, RecoveryFrame frame, Stats.Operation operation) {
        var ticket = frame.ticket();
        if (!scopes.awaiting(ticket) || !frame.owner().getAsBoolean()) {
            return false;
        }
        if (continuations != null) {
            BooleanSupplier waiting = () -> scopes.awaiting(ticket) && frame.owner().getAsBoolean();
            var continued = continuations.resolveExisting(pipelineId, frame.owner());
            if (continued.isEmpty()) {
                if (!continuations.prepareHandoff(pipelineId, null, waiting)) { return false; }
                continued = continuations.resolveExisting(pipelineId, frame.owner());
            }
            if (continued.isPresent()) {
                return recoverContinuation(pipelineId, frame, operation, continued.orElseThrow());
            }
        }
        var qualified = scopeRecovery.resolve(pipelineId).orElse(null);
        if (qualified == null || !scopes.awaiting(ticket) || !frame.owner().getAsBoolean()
                || !scopeRecovery.matchesOwner(pipelineId, qualified, ticket.owner())
                || !scopeRecovery.unchanged(pipelineId, qualified)) {
            return false;
        }
        BooleanSupplier current = () -> scopes.awaiting(ticket) && frame.owner().getAsBoolean()
                && scopeRecovery.matchesOwner(pipelineId, qualified, ticket.owner())
                && scopeRecovery.unchanged(pipelineId, qualified);
        operation.scope = qualified.scope();
        scopes.restorationFailureLog(ticket, qualified.checkpoint())
                .ifPresent(diagnostic -> diagnostic.emit(qualified.scope(), current));
        var prepared = scopes.restorationPrepared(ticket, qualified).orElse(null);
        if (prepared == null) {
            var attempt = scopes.restorationFailure(ticket, qualified.checkpoint());
            prepared = publisher.prepareScoped(pipelineId,
                    attempt != null && attempt.first() ? attempt.failure() : null, qualified.scope(), current)
                    .orElse(null);
            // Preparation can fail before or after its local failure account is updated. A neutral
            // retry reads that account; only a still unrecorded cause is handed back for counting.
            if (prepared != null && attempt != null && !attempt.first()
                    && prepared.observation().state() == StateJson.parse(qualified.checkpoint().stateJson())
                    && !recordedFailure(prepared, attempt.failure())) {
                prepared = publisher.prepareScoped(pipelineId, attempt.failure(), qualified.scope(), current)
                        .orElse(null);
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
        var persisted = publisher.commit(scopes.restorationFrame(ticket, qualified.stored(), prepared), scope, current);
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

    private boolean recoverContinuation(String id, RecoveryFrame frame, Stats.Operation operation,
            ObservationContinuationRecovery.ResolvedTarget target) {
        var ticket = frame.ticket();
        if (!scopes.awaiting(ticket) || !target.current().getAsBoolean()) { return false; }
        var signals = scopes.restorationSignals(ticket);
        var attempt = scopes.restorationFailure(ticket, target.checkpoint());
        // The owner and physical job remain the guard after adoption invalidates the old recovery ticket.
        BooleanSupplier current = () -> frame.owner().getAsBoolean() && target.current().getAsBoolean();
        scopes.restorationFailureLog(ticket, target.checkpoint()).ifPresent(diagnostic ->
                diagnostic.emit(target.target().scope(), () -> scopes.awaiting(ticket) && current.getAsBoolean()));
        var raw = publisher.prepareScoped(id, attempt != null && attempt.first() ? attempt.failure() : null,
                target.target().scope(), current).orElse(null);
        if (raw == null || !scopes.awaiting(ticket) || !current.getAsBoolean()
                || !continuations.adoptExisting(target)) { return false; }
        operation.scope = target.target().scope();
        // Adoption consumes the old recovery ticket. Its finite signals belong to the independently healthy event lane.
        for (var signal : signals) {
            PipelineStateEvents.of(id, operation.scope, signal.result(), signal.failure()).forEach(this::offerEvent);
        }
        var packet = continuationPacket(raw, operation.scope, current);
        if (packet.isEmpty()) { return false; }
        var persisted = commitContinuation(packet.orElseThrow().projected(), operation.scope, current, packet);
        if (persisted.isEmpty() || !current.getAsBoolean()) { return false; }
        var projected = packet.orElseThrow().projected();
        if (persisted.orElseThrow() != projected.observation()) {
            projected = new ObservationPublisher.Prepared(persisted.orElseThrow(), false,
                    projected.nestReadings(), projected.pinned(), projected.gaps());
        }
        offerProjections(projected, operation.scope);
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
            if (closed.get()) {
                return;
            }
            LatestSlot existing = latestByPipeline.get(pipelineId);
            if (existing != null) {
                Frame rejected = null;
                synchronized (existing) {
                    if (existing.retired) {
                        continue;
                    }
                    if (existing.pending != null) {
                        latestStats.coalesced.incrementAndGet();
                        if (frame instanceof PreparationFrame incoming
                                && existing.pending instanceof PreparationFrame previous) {
                            preparationDropped(previous);
                            frame = incoming.replacing(previous);
                        }
                    } else if (existing.started) {
                        existing.pendingCounted = true;
                        latestPending.incrementAndGet();
                    }
                    existing.pending = frame;
                    if (!existing.scheduled) {
                        // A parked request consumes its original capacity permit and waits for this offer.
                        // Resubmission is non-blocking and never creates another slot for this pipeline.
                        existing.scheduled = true;
                        try {
                            latestWorkers.execute(existing);
                            if (existing.pendingCounted) {
                                latestPending.decrementAndGet();
                                existing.pendingCounted = false;
                            }
                        } catch (java.util.concurrent.RejectedExecutionException saturated) {
                            existing.scheduled = false;
                            rejected = existing.pending;
                        }
                    }
                    latestStats.queueDepth(latestWorkers.getQueue().size() + latestPending.get());
                }
                if (rejected != null) {
                    latestStats.dropped();
                    if (rejected instanceof PreparationFrame preparation) {
                        preparationDropped(preparation);
                    }
                    boundaryFailed(pipelineId, rejected.scope(), Sink.LATEST);
                    LOG.warn("Latest observation for pipeline {} is waiting for telemetry worker capacity", pipelineId);
                }
                return;
            }
            if (!latestCapacity.tryAcquire()) {
                latestStats.dropped();
                if (frame instanceof PreparationFrame preparation) {
                    preparationDropped(preparation);
                }
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
                if (frame instanceof PreparationFrame preparation) {
                    preparationDropped(preparation);
                }
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
                        if (!slot.scheduled) {
                            slot.retire();
                        }
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
        latestByPipeline.values().forEach(slot -> {
            synchronized (slot) {
                if (!slot.scheduled) {
                    slot.retire();
                }
            }
        });
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
        private boolean scheduled = true;

        private LatestSlot(String pipelineId, Frame first) {
            this.pipelineId = pipelineId;
            this.pending = first;
        }

        private synchronized void cancel() {
            retire();
        }

        /** Called with the slot monitor; the capacity permit is released exactly once. */
        private void retire() {
            if (retired) {
                return;
            }
            retired = true;
            scheduled = false;
            pending = null;
            if (pendingCounted) {
                latestPending.decrementAndGet();
                pendingCounted = false;
            }
            latestByPipeline.remove(pipelineId, this);
            latestCapacity.release();
        }

        private void park(PreparationFrame request) {
            synchronized (this) {
                if (retired) {
                    return;
                }
                if (pending instanceof PreparationFrame incoming) {
                    pending = incoming.replacing(request);
                } else if (pending == null || Objects.equals(request.scope(), pending.scope())) {
                    pending = request;
                }
                scheduled = false;
                started = false;
                if (pending != null && !pendingCounted) {
                    latestPending.incrementAndGet();
                    pendingCounted = true;
                }
            }
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
                        retire();
                        return;
                    }
                }
                if (!latestStats.allow()) {
                    latestStats.dropped();
                    if (frame instanceof PreparationFrame preparation) {
                        preparationDropped(preparation);
                    }
                    boundaryFailed(pipelineId, frame.scope(), Sink.LATEST);
                    continue;
                }
                Stats.Operation operation = latestStats.begin(pipelineId, frame.scope(),
                        frame instanceof PreparationFrame preparation ? preparation : null);
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
                    } else if (frame instanceof PreparationFrame preparation) {
                        PreparationOutcome outcome = prepareAndCommit(preparation, operation);
                        if (outcome == PreparationOutcome.PUBLISHED) {
                            successfulCompletion(latestStats, operation, Sink.LATEST);
                        } else {
                            latestStats.skipped(operation);
                            if (outcome == PreparationOutcome.RETRY) {
                                park(preparation);
                                return;
                            }
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
                    if (frame instanceof PreparationFrame preparation && preparation.failure() != null
                            && !operation.failureCaptured
                            && qualification(preparation) != PublicationQualification.STALE) {
                        park(preparation);
                        return;
                    }
                } catch (Error defect) {
                    latestStats.completed(operation, false);
                    throw defect;
                }
            }
        }
    }
}
