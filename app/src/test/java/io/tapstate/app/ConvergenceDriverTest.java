package io.tapstate.app;

import ch.qos.logback.classic.Logger;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.control.core.PipelineExplanation.PendingReason;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.runtime.scheduler.StartDeferred;
import io.tapstate.runtime.engine.StoredCountSampler;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineEventStore;
import io.tapstate.spi.store.StateStore;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Clock;
import java.time.Instant;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapstate.core.lifecycle.PipelineState.FAILED;
import static io.tapstate.core.lifecycle.PipelineState.NEW;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * The convergence driver ticks the framework-free converger over every desired pipeline. It reconciles
 * each one toward its intent, and isolates a per-pipeline failure so one bad pipeline cannot starve the
 * rest of the pass.
 */
class ConvergenceDriverTest {

    @Test
    void completedTransitionsOfferOneScopedFailureAndRecoveryTrace() throws Exception {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        FailingActuator actuator = new FailingActuator();
        PipelineConverger loop = new PipelineConverger(desired, state, actuator,
                Clock.fixed(T0, ZoneOffset.UTC));
        ObservationStore latest = new ObservationStore() {
            @Override public void save(Observation observation) { observations.save(observation); }
            @Override public boolean saveScoped(Observation observation, Scope scope) {
                observations.save(observation);
                return true;
            }
            @Override public Optional<Observation> read(String id) { return observations.read(id); }
            @Override public void delete(String id) { observations.delete(id); }
        };
        List<PipelineEvent> persisted = new CopyOnWriteArrayList<>();
        PipelineEventStore events = new PipelineEventStore() {
            @Override public void append(PipelineEvent event) { persisted.add(event); }
            @Override public Page readPage(String id, String incarnation, Instant from, Instant to,
                    Key after, int limit) { return new Page(List.of(), false); }
            @Override public void deleteIncarnation(String id, String incarnation) { }
            @Override public Duration retention() { return Duration.ofDays(15); }
        };
        ObservationPublisher publisher = new ObservationPublisher(state, latest);
        try (TelemetryDispatcher telemetry = new TelemetryDispatcher(
                publisher, null, MetricsExport.none(), scopes, events, 1, 8)) {
            ConvergenceDriver driver = new ConvergenceDriver(loop, desired, publisher, null,
                    MetricsExport.none(), () -> true, PipelineActuationOwnership.single(),
                    LifecycleWorkDispatcher.inline(), scopes, telemetry);
            scopes.begin("orders", "inc-a", 1);
            desired.save(new DesiredState("orders", RUNNING, "rev-1"));
            driver.reconcile();
            awaitEvents(persisted, 1);

            actuator.failWith(new IllegalStateException("sink write failed"));
            driver.reconcile();
            awaitEvents(persisted, 3);
            actuator.failWith(null);
            desired.save(new DesiredState("orders", STOPPED, "rev-2"));
            driver.reconcile();
            awaitEvents(persisted, 4);
            scopes.begin("orders", "inc-a", 2);
            desired.save(new DesiredState("orders", RUNNING, "rev-3"));
            driver.reconcile();
            awaitEvents(persisted, 7);
            driver.reconcile();

            assertThat(persisted).extracting(PipelineEvent::kind).containsExactly(
                    PipelineEvent.Kind.STATE_CHANGED,
                    PipelineEvent.Kind.STATE_CHANGED, PipelineEvent.Kind.FAILURE,
                    PipelineEvent.Kind.STATE_CHANGED,
                    PipelineEvent.Kind.STATE_CHANGED, PipelineEvent.Kind.EXECUTION_RECOVERED,
                    PipelineEvent.Kind.EXECUTION_RESTARTED);
            assertThat(persisted.get(2).failure()).isNotNull();
            assertThat(persisted.get(2).failure().code()).isEqualTo("engine.job-failed");
            assertThat(persisted).extracting(PipelineEvent::pipelineIncarnationId).containsOnly("inc-a");
            assertThat(persisted.subList(4, 7)).extracting(PipelineEvent::executionGeneration)
                    .containsOnly(2L);
        }
    }

    private static void awaitEvents(List<PipelineEvent> events, int count) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (events.size() < count && System.nanoTime() - deadline < 0) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
        assertThat(events).hasSize(count);
    }

    @Test
    void defaultFourWorkersFairlyRetryARefusedPipelineDespiteRecurringQueuedCompetition() throws Exception {
        assertThat(LifecycleWorkDispatcher.DEFAULT_MAX_CONCURRENCY).isEqualTo(4);
        for (int index = 0; index < 6; index++) {
            String id = "fair-" + index;
            desired.save(new DesiredState(id, RUNNING, "rev-1"));
        }
        List<String> order = desired.pipelineIds();
        assertThat(order).hasSize(6);
        Set<String> holders = Set.copyOf(order.subList(0, 4));
        String competitor = order.get(4);
        String victim = order.get(5);
        CountDownLatch holdersEntered = new CountDownLatch(4);
        CountDownLatch releaseHolders = new CountDownLatch(1);
        CountDownLatch victimSubmitted = new CountDownLatch(1);
        Set<String> preparing = ConcurrentHashMap.newKeySet();
        Set<String> carrying = ConcurrentHashMap.newKeySet();
        Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        AtomicInteger peakPreparing = new AtomicInteger();
        // The fixture store uses a plain map. Serialize only its operations, never the blocked callbacks.
        StateStore checkpoints = new StateStore() {
            @Override public synchronized Optional<CheckpointDoc> read(String id) { return state.read(id); }
            @Override public synchronized void create(String id, String json, Instant at) { state.create(id, json, at); }
            @Override public synchronized void delete(String id) { state.delete(id); }
            @Override public synchronized CasOutcome compareAndSwap(String id, long epoch, String json, Instant at) {
                return state.compareAndSwap(id, epoch, json, at);
            }
        };
        LifecycleActuator blocked = new LifecycleActuator() {
            @Override public PreparedStart prepareStart(String id) {
                calls.computeIfAbsent(id, ignored -> new AtomicInteger()).incrementAndGet();
                if (!preparing.add(id)) { throw new AssertionError("one pipeline entered overlapping start callbacks"); }
                peakPreparing.accumulateAndGet(preparing.size(), Math::max);
                if (holders.contains(id)) {
                    holdersEntered.countDown();
                    try {
                        if (!releaseHolders.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("the four active starts were not released");
                        }
                    } catch (InterruptedException interrupted) {
                        preparing.remove(id);
                        Thread.currentThread().interrupt();
                        throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
                    }
                }
                return new PreparedStart() {
                    @Override public void submit() {
                        carrying.add(id);
                        if (id.equals(victim)) { victimSubmitted.countDown(); }
                    }
                    @Override public void close() { preparing.remove(id); }
                };
            }
            @Override public void start(String id) { throw new AssertionError("a start was not prepared"); }
            @Override public void pause(String id) { }
            @Override public void resume(String id) { }
            @Override public void stop(String id, boolean purge) { }
            @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
            @Override public Optional<Throwable> lost(String id) { return Optional.empty(); }
            @Override public boolean isCarryingAJob(String id) { return carrying.contains(id); }
        };
        PipelineConverger loop = new PipelineConverger(desired, checkpoints, blocked, Clock.fixed(T0, ZoneOffset.UTC));
        LifecyclePendingRegistry pending = new LifecyclePendingRegistry();
        try (LifecycleWorkDispatcher work = new LifecycleWorkDispatcher(
                LifecycleWorkDispatcher.DEFAULT_MAX_CONCURRENCY, 1)) {
            ConvergenceDriver isolated = new ConvergenceDriver(loop, desired,
                    new ObservationPublisher(checkpoints, observations), null, MetricsExport.none(),
                    () -> true, PipelineActuationOwnership.single(), work, null, null, pending);
            try {
                isolated.reconcile();
                assertThat(holdersEntered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(work.health().activeSlots()).isEqualTo(4);
                assertThat(work.health().queueDepth()).isEqualTo(1);
                assertThat(work.activeCount()).isEqualTo(5);
                assertThat(preparing).containsExactlyInAnyOrderElementsOf(holders);
                assertThat(work.current(competitor, desired.read(competitor).orElseThrow())).isTrue();
                assertThat(pending.pending(victim).orElseThrow().reason()).isEqualTo(PendingReason.START_CAPACITY);

                // No slot is released on this pass: all existing offers must coalesce without another callback.
                isolated.reconcile();
                assertThat(work.health().coalesced()).isGreaterThanOrEqualTo(5L);
                holders.forEach(id -> assertThat(calls.get(id)).hasValue(1));
                assertThat(calls).doesNotContainKeys(competitor, victim);
                assertThat(work.health().activeSlots()).isEqualTo(4);
                assertThat(work.health().queueDepth()).isEqualTo(1);
                assertThat(releaseHolders.getCount()).isEqualTo(1L);

                boolean admitted = false;
                for (int pass = 0; pass < order.size() && !admitted; pass++) {
                    // Replace only this real queued work. The four running callbacks never leave their slots.
                    work.cancel(competitor);
                    LifecycleWorkDispatcher.Outcome cancelled = work.take(competitor);
                    assertThat(cancelled).isNotNull();
                    assertThat(cancelled.superseded()).isTrue();
                    assertThat(work.health().activeSlots()).isEqualTo(4);
                    assertThat(work.health().queueDepth()).isZero();
                    assertThat(work.activeCount()).isEqualTo(4);
                    isolated.reconcile();
                    assertThat(work.health().activeSlots()).isEqualTo(4);
                    assertThat(work.health().queueDepth()).isLessThanOrEqualTo(1);
                    assertThat(work.activeCount()).isEqualTo(5);
                    holders.forEach(id -> assertThat(calls.get(id)).hasValue(1));
                    holders.forEach(id -> assertThat(checkpoints.read(id)
                            .map(doc -> StateJson.parse(doc.stateJson()))).contains(NEW));
                    assertThat(checkpoints.read(competitor)).isEmpty();
                    assertThat(checkpoints.read(victim)).isEmpty();
                    order.forEach(id -> observations.read(id).ifPresent(observation ->
                            assertThat(observation.state())
                                    .as("preparation and capacity do not invent a running or failed observation")
                                    .isEqualTo(NEW)));
                    assertThat(observations.read(competitor)).isEmpty();
                    assertThat(observations.read(victim)).isEmpty();
                    admitted = work.current(victim, desired.read(victim).orElseThrow());
                    assertThat(pending.pending(victim).orElseThrow().reason())
                            .isEqualTo(admitted ? PendingReason.START_PENDING : PendingReason.START_CAPACITY);
                }
                assertThat(admitted)
                        .as("a capacity-refused pipeline gets the only free slot within one complete scan turn")
                        .isTrue();
                assertThat(calls).doesNotContainKeys(competitor, victim);
                assertThat(releaseHolders.getCount()).isEqualTo(1L);
                assertThat(peakPreparing).hasValue(4);

                releaseHolders.countDown();
                awaitStateAndObservation(isolated, victim, RUNNING, checkpoints);
                assertThat(victimSubmitted.getCount()).isZero();
                assertThat(checkpoints.read(victim).orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
                assertThat(observations.read(victim).orElseThrow().state()).isEqualTo(RUNNING);
                assertThat(calls.get(victim)).hasValue(1);
                assertThat(peakPreparing.get()).isLessThanOrEqualTo(4);
            } finally {
                releaseHolders.countDown();
            }
        }
    }

    @Test
    void aStopTeardownWaitingForItsFullBudgetLeavesAnotherPipelineConverging() throws Exception {
        CountDownLatch slowStopEntered = new CountDownLatch(1);
        CountDownLatch releaseSlowStop = new CountDownLatch(1);
        LifecycleActuator blocking = new LifecycleActuator() {
            @Override public void start(String pipelineId) { }
            @Override public void pause(String pipelineId) { }
            @Override public void resume(String pipelineId) { }
            @Override public void stop(String pipelineId, boolean purgeState) {
                if (pipelineId.equals("slow")) {
                    slowStopEntered.countDown();
                    try {
                        if (!releaseSlowStop.await(35, TimeUnit.SECONDS)) {
                            throw new AssertionError("slow teardown was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
            }
            @Override public Optional<Throwable> failure(String pipelineId) { return Optional.empty(); }
            @Override public Optional<Throwable> lost(String pipelineId) { return Optional.empty(); }
            @Override public boolean isCarryingAJob(String pipelineId) { return true; }
        };
        desired.save(new DesiredState("slow", RUNNING, "rev-1"));
        desired.save(new DesiredState("fast", RUNNING, "rev-1"));
        PipelineConverger loop = new PipelineConverger(desired, state, blocking,
                Clock.fixed(T0, ZoneOffset.UTC));
        LifecyclePendingRegistry pending = new LifecyclePendingRegistry();
        try (LifecycleWorkDispatcher work = new LifecycleWorkDispatcher(2, 2)) {
            ConvergenceDriver isolated = new ConvergenceDriver(loop, desired,
                    new ObservationPublisher(state, observations), null, MetricsExport.none(),
                    () -> true, PipelineActuationOwnership.single(), work, null, null, pending);
            awaitStateAndObservation(isolated, "slow", RUNNING);
            awaitStateAndObservation(isolated, "fast", RUNNING);

            desired.save(new DesiredState("slow", STOPPED, "rev-2"));
            long stopDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (slowStopEntered.getCount() != 0 && System.nanoTime() - stopDeadline < 0) {
                isolated.reconcile();
                Thread.sleep(10);
            }
            assertThat(slowStopEntered.getCount()).isZero();
            long slowStopStarted = System.nanoTime();
            assertThat(pending.pending("slow").orElseThrow().reason()).isEqualTo(PendingReason.STOP_PENDING);

            desired.save(new DesiredState("fast", io.tapstate.core.lifecycle.PipelineState.PAUSED, "rev-2"));
            awaitStateAndObservation(isolated, "fast", io.tapstate.core.lifecycle.PipelineState.PAUSED);
            desired.save(new DesiredState("fast", RUNNING, "rev-3"));
            awaitStateAndObservation(isolated, "fast", RUNNING);
            assertThat(releaseSlowStop.getCount()).isEqualTo(1L);
            if (Boolean.getBoolean("tapstate.e2e.long-stop-witness")) {
                Instant beforeLongWait = observations.read("fast").orElseThrow().observedAt();
                long fullBudget = slowStopStarted + TimeUnit.SECONDS.toNanos(30);
                while (System.nanoTime() - fullBudget < 0) {
                    isolated.reconcile();
                    Thread.sleep(250);
                }
                Instant afterLongWait = observations.read("fast").orElseThrow().observedAt();
                assertThat(afterLongWait).isAfter(beforeLongWait);
                assertThat(releaseSlowStop.getCount()).isEqualTo(1L);
                assertThat(state.read("fast").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
                System.out.printf("long-stop-isolation heldMs=%.3f fastObservedBefore=%s fastObservedAfter=%s%n",
                        (System.nanoTime() - slowStopStarted) / 1_000_000.0,
                        beforeLongWait, afterLongWait);
            }
        } finally {
            releaseSlowStop.countDown();
        }
    }

    private void awaitStateAndObservation(ConvergenceDriver driver, String pipelineId,
            io.tapstate.core.lifecycle.PipelineState expected) throws InterruptedException {
        awaitStateAndObservation(driver, pipelineId, expected, state);
    }

    private void awaitStateAndObservation(ConvergenceDriver driver, String pipelineId,
            io.tapstate.core.lifecycle.PipelineState expected, StateStore checkpoints) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() - deadline < 0) {
            driver.reconcile();
            if (checkpoints.read(pipelineId).map(checkpoint -> StateJson.parse(checkpoint.stateJson()))
                    .filter(expected::equals).isPresent()
                    && observations.read(pipelineId).map(Observation::state)
                            .filter(expected::equals).isPresent()) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("pipeline " + pipelineId + " did not reach " + expected);
    }

    @Test
    void stopAndDeleteInterruptAnInFlightStartBeforeItCanSubmit() throws Exception {
        for (boolean delete : new boolean[] {false, true}) {
            String id = delete ? "deleted" : "stopped";
            InMemoryDesiredStore intents = new InMemoryDesiredStore();
            InMemoryStateStore checkpoints = new InMemoryStateStore();
            InMemoryObservationStore latest = new InMemoryObservationStore();
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicBoolean interrupted = new AtomicBoolean();
            AtomicBoolean submitted = new AtomicBoolean();
            AtomicInteger stops = new AtomicInteger();
            LifecycleActuator actuator = new LifecycleActuator() {
                @Override public PreparedStart prepareStart(String pipelineId) {
                    entered.countDown();
                    try {
                        if (!release.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("blocked start was not released");
                        }
                    } catch (InterruptedException cancelled) {
                        interrupted.set(true);
                        Thread.currentThread().interrupt();
                        throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
                    }
                    return new PreparedStart() {
                        @Override public void submit() { submitted.set(true); }
                        @Override public void close() { }
                    };
                }
                @Override public void start(String pipelineId) { throw new AssertionError("unprepared start"); }
                @Override public void pause(String pipelineId) { }
                @Override public void resume(String pipelineId) { }
                @Override public void stop(String pipelineId, boolean purgeState) { stops.incrementAndGet(); }
                @Override public Optional<Throwable> failure(String pipelineId) { return Optional.empty(); }
                @Override public Optional<Throwable> lost(String pipelineId) { return Optional.empty(); }
                @Override public boolean isCarryingAJob(String pipelineId) { return submitted.get(); }
            };
            intents.save(new DesiredState(id, RUNNING, "rev-1"));
            PipelineConverger loop = new PipelineConverger(intents, checkpoints, actuator,
                    Clock.fixed(T0, ZoneOffset.UTC));
            LifecyclePendingRegistry pending = new LifecyclePendingRegistry();
            try (LifecycleWorkDispatcher work = new LifecycleWorkDispatcher(1, 1)) {
                ConvergenceDriver isolated = new ConvergenceDriver(loop, intents,
                        new ObservationPublisher(checkpoints, latest), null, MetricsExport.none(),
                        () -> true, PipelineActuationOwnership.single(), work, null, null, pending);
                isolated.reconcile();
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                if (delete) {
                    intents.delete(id);
                } else {
                    intents.save(new DesiredState(id, STOPPED, "rev-2"));
                }

                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (System.nanoTime() - deadline < 0) {
                    isolated.reconcile();
                    if (delete ? work.activeCount() == 0
                            : checkpoints.read(id).map(checkpoint -> StateJson.parse(checkpoint.stateJson()))
                                    .filter(STOPPED::equals).isPresent()
                                    && work.activeCount() == 0) {
                        break;
                    }
                    Thread.sleep(10);
                }
                assertThat(work.activeCount()).isZero();
                assertThat(submitted).isFalse();
                assertThat(interrupted).isTrue();
                assertThat(pending.pending(id)).isEmpty();
                assertThat(stops).hasValue(delete ? 0 : 1);
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void aDeferredSnapshotStartKeepsItsCapacityReasonUntilAdmissionSucceeds() {
        AtomicBoolean hasSlot = new AtomicBoolean(false);
        LifecycleActuator actuator = new LifecycleActuator() {
            @Override public PreparedStart prepareStart(String pipelineId) {
                if (!hasSlot.get()) {
                    throw new StartDeferred(StartDeferred.Reason.CAPACITY);
                }
                return LifecycleActuator.super.prepareStart(pipelineId);
            }
            @Override public void start(String pipelineId) { }
            @Override public void pause(String pipelineId) { }
            @Override public void resume(String pipelineId) { }
            @Override public void stop(String pipelineId, boolean purgeState) { }
            @Override public Optional<Throwable> failure(String pipelineId) { return Optional.empty(); }
            @Override public Optional<Throwable> lost(String pipelineId) { return Optional.empty(); }
            @Override public boolean isCarryingAJob(String pipelineId) { return hasSlot.get(); }
        };
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        LifecyclePendingRegistry pending = new LifecyclePendingRegistry();
        ConvergenceDriver isolated = new ConvergenceDriver(
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC)),
                desired, new ObservationPublisher(state, observations), null, MetricsExport.none(),
                () -> true, PipelineActuationOwnership.single(), LifecycleWorkDispatcher.inline(),
                null, null, pending);

        isolated.reconcile();
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(NEW));
        assertThat(pending.pending("orders").orElseThrow().reason()).isEqualTo(PendingReason.START_CAPACITY);

        hasSlot.set(true);
        isolated.reconcile();
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(pending.pending("orders")).isEmpty();
    }

    @Test
    void acceptedWorkAndCapacityWaitingHaveDistinctPendingReasonsWithoutFabricatingActualState()
            throws Exception {
        CountDownLatch slowEntered = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        LifecycleActuator blocking = new LifecycleActuator() {
            @Override public void start(String id) {
                if (!id.equals("slow")) {
                    return;
                }
                slowEntered.countDown();
                try {
                    if (!releaseSlow.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("slow start was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
            @Override public void pause(String id) { }
            @Override public void resume(String id) { }
            @Override public void stop(String id, boolean purgeState) { }
            @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
            @Override public Optional<Throwable> lost(String id) { return Optional.empty(); }
            @Override public boolean isCarryingAJob(String id) { return true; }
        };
        desired.save(new DesiredState("slow", RUNNING, "rev-1"));
        desired.save(new DesiredState("queued", RUNNING, "rev-1"));
        desired.save(new DesiredState("wait", RUNNING, "rev-1"));
        desired.save(new DesiredState("stop", STOPPED, "rev-1"));
        PipelineConverger loop = new PipelineConverger(desired, state, blocking,
                Clock.fixed(T0, ZoneOffset.UTC));
        LifecyclePendingRegistry pending = new LifecyclePendingRegistry();
        try (LifecycleWorkDispatcher work = new LifecycleWorkDispatcher(1, 1)) {
            ConvergenceDriver isolated = new ConvergenceDriver(loop, desired,
                    new ObservationPublisher(state, observations), null, MetricsExport.none(), () -> true,
                    PipelineActuationOwnership.single(), work, null, null, pending);

            isolated.reconcile();

            assertThat(slowEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(pending.pending("slow").orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            assertThat(pending.pending("queued").orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            assertThat(pending.pending("wait").orElseThrow().reason()).isEqualTo(PendingReason.START_CAPACITY);
            assertThat(pending.pending("stop").orElseThrow().reason()).isEqualTo(PendingReason.STOP_CAPACITY);
            assertThat(state.read("wait")).isEmpty();
            assertThat(observations.read("wait")).isEmpty();
            releaseSlow.countDown();
        } finally {
            releaseSlow.countDown();
        }
    }

    @Test
    void aKnownFailedNoopDoesNotBecomeAQueuedStart() throws Exception {
        verifyKnownFailedNoopPending(false);
    }

    @Test
    void aKnownFailedNoopDoesNotBecomeAStartCapacityWait() throws Exception {
        verifyKnownFailedNoopPending(true);
    }

    private void verifyKnownFailedNoopPending(boolean fillQueue) throws Exception {
        CountDownLatch slowEntered = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        AtomicInteger failedStarts = new AtomicInteger();
        LifecycleActuator blocking = new LifecycleActuator() {
            @Override public void start(String id) {
                if (id.equals("orders")) {
                    failedStarts.incrementAndGet();
                }
                if (id.equals("slow")) {
                    slowEntered.countDown();
                    try {
                        if (!releaseSlow.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("the other pipeline's start was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
            }
            @Override public void pause(String id) { }
            @Override public void resume(String id) { }
            @Override public void stop(String id, boolean purgeState) { }
            @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
            @Override public Optional<Throwable> lost(String id) { return Optional.empty(); }
            @Override public boolean isCarryingAJob(String id) { return !id.equals("orders"); }
        };
        DesiredState intent = new DesiredState("orders", RUNNING, "rev-1", false,
                "assembly-1", false, null);
        desired.save(intent);
        state.create("orders", StateJson.of(FAILED), T0);
        var failed = state.read("orders").orElseThrow();
        PipelineConverger loop = new PipelineConverger(desired, state, blocking,
                Clock.fixed(T0, ZoneOffset.UTC));
        LifecyclePendingRegistry pending = new LifecyclePendingRegistry();
        try (LifecycleWorkDispatcher work = new LifecycleWorkDispatcher(1, 1)) {
            ConvergenceDriver isolated = new ConvergenceDriver(loop, desired,
                    new ObservationPublisher(state, observations), null, MetricsExport.none(), () -> true,
                    PipelineActuationOwnership.single(), work, null, null, pending);
            isolated.reconcile();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (work.health().pendingPipelines() != 0 && System.nanoTime() - deadline < 0) {
                Thread.sleep(10);
            }
            assertThat(work.health().pendingPipelines()).isZero();
            if (work.activeCount() != 0) {
                isolated.reconcile();
            }
            assertThat(work.activeCount()).isZero();
            assertThat(pending.pending("orders")).isEmpty();
            assertThat(state.read("orders")).contains(failed);

            DesiredState slow = new DesiredState("slow", RUNNING, "slow-rev");
            desired.save(slow);
            assertThat(work.offer("slow", slow, () -> loop.converge("slow")))
                    .isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);
            assertThat(slowEntered.await(5, TimeUnit.SECONDS)).isTrue();
            if (fillQueue) {
                DesiredState queued = new DesiredState("queued", RUNNING, "queued-rev");
                desired.save(queued);
                assertThat(work.offer("queued", queued, () -> loop.converge("queued")))
                        .isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);
            }

            isolated.reconcile();

            assertThat(state.read("orders")).contains(failed);
            assertThat(desired.read("orders")).contains(intent);
            assertThat(failedStarts).hasValue(0);
            assertThat(pending.pending("orders"))
                    .as("a queued periodic terminal NOOP is not a requested or admitted start")
                    .isEmpty();
            assertThat(pending.pending("slow").orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            if (fillQueue) {
                assertThat(pending.pending("queued").orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            }
            releaseSlow.countDown();
        } finally {
            releaseSlow.countDown();
        }
    }

    @Test
    void anUnspentRebuildStampStillPublishesAQueuedStartAfterTerminalNoop() throws Exception {
        verifyTerminalNoopChange(PendingChange.REBUILD);
    }

    @Test
    void anAdmittedRecoveryReplacesTerminalNoopBeforePreparingItsStart() throws Exception {
        verifyTerminalNoopChange(PendingChange.RECOVERY);
    }

    @Test
    void aDeletedIncarnationCannotHideTheSameIdsFreshQueuedStart() throws Exception {
        verifyTerminalNoopChange(PendingChange.INCARNATION);
    }

    @Test
    void continuationCleanupCannotRelabelTheSameBlockedStopAsAStart() throws Exception {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var scope = scopes.begin("orders", "inc-a", 1);
        CountDownLatch stopping = new CountDownLatch(1);
        CountDownLatch releaseStop = new CountDownLatch(1);
        var cause = new io.tapstate.core.common.TapstateException(io.tapstate.runtime.engine.EngineError.JOB_FAILED,
                Map.of("pipeline", "orders", "cause", "controlled capture failure"), null);
        LifecycleActuator actuator = new LifecycleActuator() {
            @Override public void start(String id) { }
            @Override public void pause(String id) { }
            @Override public void resume(String id) { }
            @Override public void stop(String id, boolean purgeState) {
                // The real ordinary stop clears carry before waiting for the native job to end.
                scopes.clearContinuation(id);
                stopping.countDown();
                try {
                    if (!releaseStop.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("the controlled stop was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("the controlled stop was interrupted", interrupted);
                }
            }
            @Override public Optional<Throwable> failure(String id) {
                return id.equals("orders") ? Optional.of(cause) : Optional.empty();
            }
            @Override public Optional<Throwable> lost(String id) { return Optional.empty(); }
            @Override public boolean isCarryingAJob(String id) { return true; }
        };
        DesiredState intent = new DesiredState("orders", RUNNING, "same-revision", false,
                "same-assembly", false, null);
        desired.save(intent);
        state.create("orders", StateJson.of(RUNNING), T0);
        ObservationStore latest = new ObservationStore() {
            @Override public void save(Observation observation) { observations.save(observation); }
            @Override public boolean saveScoped(Observation observation, Scope owner) {
                observations.save(observation);
                return true;
            }
            @Override public Optional<Observation> read(String id) { return observations.read(id); }
            @Override public void delete(String id) { observations.delete(id); }
        };
        LifecyclePendingRegistry pending = new LifecyclePendingRegistry();
        try (LifecycleWorkDispatcher work = new LifecycleWorkDispatcher(1, 1)) {
            ConvergenceDriver isolated = new ConvergenceDriver(
                    new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC)), desired,
                    new ObservationPublisher(state, latest), null, MetricsExport.none(), () -> true,
                    PipelineActuationOwnership.single(), work, scopes, null, pending);
            isolated.reconcile();
            assertThat(stopping.await(5, TimeUnit.SECONDS)).isTrue();
            var concluded = state.read("orders").orElseThrow();
            assertThat(StateJson.parse(concluded.stateJson())).isEqualTo(FAILED);
            assertThat(scopes.current("orders")).contains(scope);
            assertThat(pending.pending("orders").orElseThrow().reason()).isEqualTo(PendingReason.STOP_PENDING);
            assertThat(work.activeCount()).isEqualTo(1);

            isolated.reconcile();

            assertThat(state.read("orders")).contains(concluded);
            assertThat(desired.read("orders")).contains(intent);
            assertThat(scopes.current("orders")).contains(scope);
            assertThat(work.activeCount()).isEqualTo(1);
            assertThat(pending.pending("orders").orElseThrow().reason())
                    .as("continuation cleanup preserves the actual STOP decision of the same accepted work")
                    .isEqualTo(PendingReason.STOP_PENDING);
            releaseStop.countDown();
        } finally {
            releaseStop.countDown();
        }
    }

    @Test
    void aSameScopeResetInvalidatesAnOlderBindingMutation() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var scope = scopes.begin("orders", "inc-a", 1);
        var original = scopes.bindingIdentity("orders");
        AtomicInteger applied = new AtomicInteger();

        assertThat(scopes.begin("orders", "inc-a", 1)).isEqualTo(scope);
        assertThat(scopes.bindingIdentity("orders")).isEqualTo(original);

        assertThat(scopes.beginResetExecution("orders", scope)).isEqualTo(scope);
        assertThat(scopes.current("orders")).contains(scope);
        scopes.withBindingIdentity("orders", original, applied::incrementAndGet);
        assertThat(applied).as("equal scope values cannot revive the old binding lifetime").hasValue(0);
        scopes.withBindingIdentity("orders", scopes.bindingIdentity("orders"), applied::incrementAndGet);
        assertThat(applied).hasValue(1);

        var reset = scopes.bindingIdentity("orders");
        scopes.discard("orders", scope);
        assertThat(scopes.current("orders")).isEmpty();
        assertThat(scopes.begin("orders", "inc-a", 1)).isEqualTo(scope);
        scopes.withBindingIdentity("orders", reset, applied::incrementAndGet);
        assertThat(applied).as("equal scope values cannot revive a discarded binding lifetime").hasValue(1);
        scopes.withBindingIdentity("orders", scopes.bindingIdentity("orders"), applied::incrementAndGet);
        assertThat(applied).hasValue(2);
    }

    @Test
    void aBoundRunningJobKeepsItsNoActionDecisionWhileTelemetryCompletionWaits() throws Exception {
        verifyBoundPendingDecision(BoundPendingChange.UNCHANGED);
    }

    @Test
    void aRestoredSuccessorReportsNoStartAfterItsOwnBindingChangesGeneration() throws Exception {
        verifyBoundPendingDecision(BoundPendingChange.RESTORE);
    }

    @Test
    void aRestoredSuccessorCannotClearPendingForAnotherIncarnation() throws Exception {
        verifyBoundPendingDecision(BoundPendingChange.INCARNATION);
    }

    @Test
    void aRestoredSuccessorCannotClearPendingAfterItsDurableGenerationAdvancesAgain() throws Exception {
        verifyBoundPendingDecision(BoundPendingChange.GENERATION);
    }

    @Test
    void aRestoredSuccessorCannotClearPendingAfterItsMarkerIsReplaced() throws Exception {
        verifyBoundPendingDecision(BoundPendingChange.MARKER);
    }

    @Test
    void anUnchangedLocalBindingCannotHideANewerAuthoritativeGeneration() throws Exception {
        verifyBoundPendingDecision(BoundPendingChange.GENERATION_WITH_SAME_BINDING);
    }

    @Test
    void aMarkerReplacedDuringAuthorityQualificationCannotClearTheAcceptedWorkersPending() throws Exception {
        verifyBoundPendingDecision(BoundPendingChange.MARKER_DURING_PROOF);
    }

    @Test
    void aClaimedSuccessorReportsNoStartAfterRestoringItsOwnBinding() throws Exception {
        verifyBoundPendingDecision(BoundPendingChange.CLAIMED_RESTORE);
    }

    @Test
    void aReturnedOwnerCannotReuseItsEarlierClaimReceiptForTheSameExecution() throws Exception {
        verifyBoundPendingDecision(BoundPendingChange.REPLACED_CLAIM);
    }

    private enum BoundPendingChange {
        UNCHANGED, RESTORE, INCARNATION, GENERATION, MARKER, GENERATION_WITH_SAME_BINDING, MARKER_DURING_PROOF,
        CLAIMED_RESTORE, REPLACED_CLAIM
    }

    private void verifyBoundPendingDecision(BoundPendingChange change) throws Exception {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var scope = new ObservationStore.Scope("inc-a", 2);
        boolean sameBinding = change == BoundPendingChange.UNCHANGED
                || change == BoundPendingChange.GENERATION_WITH_SAME_BINDING
                || change == BoundPendingChange.MARKER_DURING_PROOF || change == BoundPendingChange.REPLACED_CLAIM;
        scopes.begin("orders", "inc-a", sameBinding ? 2 : 1);
        InMemoryWorkloadClaimStore generations = new InMemoryWorkloadClaimStore();
        boolean claimed = change == BoundPendingChange.CLAIMED_RESTORE || change == BoundPendingChange.REPLACED_CLAIM;
        var onAuthorityRead = new java.util.concurrent.atomic.AtomicReference<Runnable>(() -> { });
        io.tapstate.spi.store.ExecutionGenerationStore qualifiedGenerations = new io.tapstate.spi.store.ExecutionGenerationStore() {
            @Override public Optional<io.tapstate.spi.store.WorkloadClaim> advanceUnderClaim(
                    io.tapstate.spi.store.WorkloadClaim expected, long revision) {
                return generations.advanceUnderClaim(expected, revision);
            }
            @Override public OptionalLong advanceStandalone(String cluster, String id) {
                return generations.advanceStandalone(cluster, id);
            }
            @Override public OptionalLong currentGeneration(String cluster, String id) {
                onAuthorityRead.getAndSet(() -> { }).run();
                return generations.currentGeneration(cluster, id);
            }
        };
        ClusterMembershipGate membership;
        ClusterWorkloadClaims claims;
        PipelineActuationOwnership ownership;
        io.tapstate.spi.store.WorkloadClaim seedClaim;
        io.tapstate.spi.store.StopAuthority authority;
        if (claimed) {
            ClusterProperties properties = new ClusterProperties();
            properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
            membership = new ClusterMembershipGate(properties);
            membership.install(new io.tapstate.spi.store.ClusterMembership("cluster-a", 7,
                    java.util.Set.of("node-a", "node-b", "node-c")));
            assertThat(membership.canCommit(java.util.Set.of("node-a", "node-b"))).isTrue();
            claims = new ClusterWorkloadClaims(generations, membership);
            var key = new io.tapstate.spi.store.WorkloadClaimKey("cluster-a",
                    io.tapstate.spi.store.WorkloadClaimType.PIPELINE_ACTUATION, "orders");
            var owner = new io.tapstate.spi.store.WorkloadOwner("node-a", "boot-a");
            var acquired = claims.acquire(key, owner, 7, Duration.ofSeconds(30)).orElseThrow();
            assertThat(acquired.acquired()).isTrue();
            var firstRun = claims.advanceExecution(acquired.claim(), 7,
                    java.util.Set.of("node-a", "node-b")).orElseThrow();
            seedClaim = claims.advanceExecution(firstRun, 7, java.util.Set.of("node-a", "node-b")).orElseThrow();
            assertThat(seedClaim.executionGeneration()).isEqualTo(2);
            ownership = new PipelineActuationOwnership("cluster-a", owner, membership, claims,
                    Duration.ofSeconds(30), Duration.ofSeconds(10), () -> 0L);
            authority = io.tapstate.spi.store.StopAuthority.claimed(io.tapstate.spi.store.WorkloadClaimFence.from(seedClaim));
        } else {
            membership = null;
            claims = null;
            seedClaim = null;
            assertThat(generations.advanceStandalone("cluster-a", "orders")).hasValue(1);
            assertThat(generations.advanceStandalone("cluster-a", "orders")).hasValue(2);
            ownership = PipelineActuationOwnership.single("cluster-a", qualifiedGenerations);
            authority = io.tapstate.spi.store.StopAuthority.standalone("cluster-a", 2);
        }
        DesiredState intent = new DesiredState("orders", RUNNING, "rev-1");
        desired.save(intent);
        state.create("orders", StateJson.of(RUNNING), T0);
        state.compareAndSwap("orders", 0, StateJson.of(RUNNING), T0);
        state.compareAndSwap("orders", 1, StateJson.of(RUNNING), T0);
        var checkpoint = state.read("orders").orElseThrow();
        String successorBoot = claimed ? "boot-a" : "successor-boot";
        var job = new io.tapstate.spi.store.StopReservation.JobIdentity("cluster-a", 102, successorBoot);
        var marker = new io.tapstate.spi.store.StopReservation("orders", "handoff-a", 0, 2, intent,
                new io.tapstate.spi.store.StopReservation.Source("cluster-a", new ObservationStore.Scope("inc-a", 1),
                        new io.tapstate.spi.store.StopReservation.JobIdentity("cluster-a", 101, "source-boot")),
                io.tapstate.spi.store.StopReservation.Phase.SUCCESSOR_BOUND,
                io.tapstate.spi.store.StopReservation.CounterPolicy.CONTINUE, authority,
                new io.tapstate.spi.store.StopReservation.Successor(scope, successorBoot, job),
                io.tapstate.spi.store.StopReservation.CURRENT_FORMAT);
        if (claimed) {
            var admitted = claimedAdmission(marker, authority);
            assertThat(ownership.adoptAdmission(new io.tapstate.spi.store.SuccessorAdmission(admitted,
                    Optional.of(seedClaim))).allowed()).isTrue();
            assertThat(ownership.permit("orders").claim().claimGeneration()).isEqualTo(1);
        }
        var savedMarker = new java.util.concurrent.atomic.AtomicReference<>(marker);
        io.tapstate.spi.store.StateStore handoff = new io.tapstate.spi.store.StateStore() {
            @Override public Optional<io.tapstate.core.lifecycle.CheckpointDoc> read(String id) { return state.read(id); }
            @Override public void create(String id, String json, Instant at) { state.create(id, json, at); }
            @Override public void delete(String id) { state.delete(id); }
            @Override public io.tapstate.core.lifecycle.CasOutcome compareAndSwap(
                    String id, long epoch, String json, Instant at) { return state.compareAndSwap(id, epoch, json, at); }
            @Override public boolean supportsStopReservations() { return true; }
            @Override public Optional<io.tapstate.spi.store.StopReservation> readStopReservation(String id) {
                return id.equals("orders") ? Optional.of(savedMarker.get()) : Optional.empty();
            }
        };
        CountDownLatch adopting = new CountDownLatch(1);
        CountDownLatch releaseAdoption = new CountDownLatch(1);
        AtomicInteger starts = new AtomicInteger();
        LifecycleActuator actuator = new LifecycleActuator() {
            @Override public void start(String id) { starts.incrementAndGet(); }
            @Override public void pause(String id) { throw new AssertionError("unexpected pause"); }
            @Override public void resume(String id) { throw new AssertionError("unexpected resume"); }
            @Override public void stop(String id, boolean purgeState) { throw new AssertionError("unexpected stop"); }
            @Override public boolean isCarryingAJob(String id) { return true; }
            @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
            @Override public Optional<Throwable> lost(String id) { return Optional.empty(); }
            @Override public Optional<io.tapstate.spi.store.StopAuthority> stopAuthority(String id) {
                return Optional.of(authority);
            }
            @Override public Optional<SuccessorInspection> inspectSuccessor(
                    io.tapstate.spi.store.StopReservation expected, java.util.function.BooleanSupplier current) {
                assertThat(expected).isEqualTo(marker);
                return current.getAsBoolean() ? Optional.of(new SuccessorInspection(Optional.of(job), Optional.empty()))
                        : Optional.empty();
            }
            @Override public boolean adoptSuccessor(
                    io.tapstate.spi.store.StopReservation expected, java.util.function.BooleanSupplier current) {
                scopes.begin("orders", "inc-a", 2);
                if (change == BoundPendingChange.GENERATION_WITH_SAME_BINDING) {
                    assertThat(generations.advanceStandalone("cluster-a", "orders")).hasValue(3);
                } else if (change == BoundPendingChange.MARKER_DURING_PROOF) {
                    onAuthorityRead.set(() -> savedMarker.set(new io.tapstate.spi.store.StopReservation(
                            marker.pipelineId(), "handoff-b", marker.sourceEpoch(), marker.reservedEpoch(),
                            marker.originalDesired(), marker.source(), marker.phase(), marker.counterPolicy(),
                            marker.writerAuthority(), marker.successor(), marker.formatVersion())));
                } else if (change == BoundPendingChange.REPLACED_CLAIM) {
                    var held = ownership.permit("orders").claim();
                    assertThat(claims.release(held)).isTrue();
                    var other = claims.acquire(held.key(), new io.tapstate.spi.store.WorkloadOwner("node-b", "boot-b"),
                            7, Duration.ofSeconds(30)).orElseThrow().claim();
                    assertThat(other.claimGeneration()).isEqualTo(2);
                    assertThat(claims.release(other)).isTrue();
                    var returned = claims.acquire(held.key(), held.owner(), 7, Duration.ofSeconds(30)).orElseThrow().claim();
                    assertThat(returned.claimGeneration()).isEqualTo(3);
                    assertThat(returned.executionGeneration()).isEqualTo(2);
                    var freshAuthority = io.tapstate.spi.store.StopAuthority.claimed(
                            io.tapstate.spi.store.WorkloadClaimFence.from(returned));
                    assertThat(ownership.adoptAdmission(new io.tapstate.spi.store.SuccessorAdmission(
                            claimedAdmission(marker, freshAuthority), Optional.of(returned))).allowed()).isTrue();
                }
                return current.getAsBoolean();
            }
            @Override public Optional<io.tapstate.spi.store.HandoffIdentity> continuationReady(
                    io.tapstate.spi.store.StopReservation expected, java.util.function.BooleanSupplier current) {
                adopting.countDown();
                try {
                    if (!releaseAdoption.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("the running successor's adoption was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("the controlled adoption was interrupted", interrupted);
                }
                return Optional.empty();
            }
        };
        LifecyclePendingRegistry pending = new LifecyclePendingRegistry();
        ObservationStore scopedLatest = new ObservationStore() {
            @Override public void save(Observation observation) { observations.save(observation); }
            @Override public boolean saveScoped(Observation observation, Scope owner) {
                observations.save(observation);
                return true;
            }
            @Override public Optional<Observation> read(String id) { return observations.read(id); }
            @Override public void delete(String id) { observations.delete(id); }
        };
        try (LifecycleWorkDispatcher work = new LifecycleWorkDispatcher(1, 1)) {
            ConvergenceDriver isolated = new ConvergenceDriver(
                    new PipelineConverger(desired, handoff, actuator, Clock.fixed(T0, ZoneOffset.UTC)), desired,
                    new ObservationPublisher(handoff, scopedLatest), null, MetricsExport.none(),
                    claimed ? membership::businessEligible : () -> true,
                    ownership, work, scopes, null, pending);
            isolated.reconcile();
            assertThat(adopting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(work.activeCount()).isEqualTo(1);

            if (change == BoundPendingChange.INCARNATION) {
                scopes.forgetIncarnation("orders", "inc-a");
                scopes.begin("orders", "inc-b", 2);
            } else if (change == BoundPendingChange.GENERATION) {
                assertThat(generations.advanceStandalone("cluster-a", "orders")).hasValue(3);
                scopes.begin("orders", "inc-a", 3);
            } else if (change == BoundPendingChange.MARKER) {
                savedMarker.set(new io.tapstate.spi.store.StopReservation(marker.pipelineId(), "handoff-b",
                        marker.sourceEpoch(), marker.reservedEpoch(), marker.originalDesired(), marker.source(),
                        marker.phase(), marker.counterPolicy(), marker.writerAuthority(), marker.successor(),
                        marker.formatVersion()));
                scopes.beginResetExecution("orders", scope);
            }

            isolated.reconcile();

            assertThat(state.read("orders")).contains(checkpoint);
            assertThat(handoff.readStopReservation("orders")).contains(savedMarker.get());
            assertThat(desired.read("orders")).contains(intent);
            assertThat(starts).hasValue(0);
            Observation latest = observations.read("orders").orElseThrow();
            assertThat(latest.state()).isEqualTo(RUNNING);
            assertThat(latest.failure()).as("a telemetry fixture failure cannot erase pending and make this pass")
                    .isNull();
            if (change == BoundPendingChange.UNCHANGED || change == BoundPendingChange.RESTORE
                    || change == BoundPendingChange.CLAIMED_RESTORE) {
                assertThat(scopes.current("orders")).contains(scope);
                assertThat(pending.pending("orders"))
                        .as("a coalesced tick cannot invent start while a known bound job waits only for telemetry")
                        .isEmpty();
            } else {
                assertThat(pending.pending("orders"))
                        .as("an old handoff decision cannot erase the new context's provisional pending")
                        .contains(new io.tapstate.control.core.PipelineExplanation.Pending(PendingReason.START_PENDING));
            }
            assertThat(generations.currentGeneration("cluster-a", "orders"))
                    .hasValue(change == BoundPendingChange.GENERATION
                            || change == BoundPendingChange.GENERATION_WITH_SAME_BINDING ? 3 : 2);
            if (claimed) {
                assertThat(ownership.permit("orders").claim().claimGeneration())
                        .isEqualTo(change == BoundPendingChange.REPLACED_CLAIM ? 3 : 1);
            }
            releaseAdoption.countDown();
        } finally {
            releaseAdoption.countDown();
        }
    }

    private static io.tapstate.spi.store.StopReservation claimedAdmission(
            io.tapstate.spi.store.StopReservation bound, io.tapstate.spi.store.StopAuthority authority) {
        return new io.tapstate.spi.store.StopReservation(bound.pipelineId(), bound.token(), bound.sourceEpoch(),
                bound.reservedEpoch(), bound.originalDesired(), bound.source(),
                io.tapstate.spi.store.StopReservation.Phase.SUCCESSOR_ADMITTED, bound.counterPolicy(), authority,
                new io.tapstate.spi.store.StopReservation.Successor(bound.successor().scope(),
                        bound.successor().submissionBootId(), null), bound.formatVersion());
    }

    private enum PendingChange { REBUILD, RECOVERY, INCARNATION }

    private void verifyTerminalNoopChange(PendingChange change) throws Exception {
        CountDownLatch slowEntered = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        CountDownLatch preparingRecovery = new CountDownLatch(1);
        CountDownLatch releaseRecovery = new CountDownLatch(1);
        AtomicBoolean recover = new AtomicBoolean();
        AtomicInteger admissions = new AtomicInteger();
        AtomicInteger starts = new AtomicInteger();
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        scopes.begin("orders", "inc-a", 1);
        LifecycleActuator blocking = new LifecycleActuator() {
            @Override public PreparedStart prepareStart(String id) {
                if (id.equals("orders") && change == PendingChange.RECOVERY) {
                    preparingRecovery.countDown();
                    try {
                        if (!releaseRecovery.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("the admitted recovery preparation was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
                return LifecycleActuator.super.prepareStart(id);
            }
            @Override public void start(String id) {
                if (id.equals("orders")) { starts.incrementAndGet(); }
                if (id.equals("slow")) {
                    slowEntered.countDown();
                    try {
                        if (!releaseSlow.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("the other pipeline's start was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
            }
            @Override public void pause(String id) { }
            @Override public void resume(String id) { }
            @Override public void stop(String id, boolean purgeState) { }
            @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
            @Override public Optional<Throwable> lost(String id) { return Optional.empty(); }
            @Override public boolean isCarryingAJob(String id) { return !id.equals("orders"); }
        };
        DesiredState original = new DesiredState("orders", RUNNING, "same-revision", false,
                "same-assembly", false, null);
        desired.save(original);
        state.create("orders", StateJson.of(FAILED), T0);
        var failed = state.read("orders").orElseThrow();
        PipelineConverger loop = new PipelineConverger(desired, state, blocking,
                Clock.fixed(T0, ZoneOffset.UTC), id -> {
                    if (id.equals("orders") && recover.get()) {
                        admissions.incrementAndGet();
                        return true;
                    }
                    return false;
                });
        ObservationStore latest = new ObservationStore() {
            @Override public void save(Observation observation) { observations.save(observation); }
            @Override public boolean saveScoped(Observation observation, Scope scope) {
                observations.save(observation);
                return true;
            }
            @Override public Optional<Observation> read(String id) { return observations.read(id); }
            @Override public void delete(String id) { observations.delete(id); }
        };
        LifecyclePendingRegistry pending = new LifecyclePendingRegistry();
        try (LifecycleWorkDispatcher work = new LifecycleWorkDispatcher(1, 1)) {
            ConvergenceDriver isolated = new ConvergenceDriver(loop, desired,
                    new ObservationPublisher(state, latest), null, MetricsExport.none(), () -> true,
                    PipelineActuationOwnership.single(), work, scopes, null, pending);
            isolated.reconcile();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (work.health().pendingPipelines() != 0 && System.nanoTime() - deadline < 0) {
                Thread.sleep(10);
            }
            assertThat(work.health().pendingPipelines()).isZero();
            if (work.activeCount() != 0) { isolated.reconcile(); }
            assertThat(work.activeCount()).isZero();
            assertThat(pending.pending("orders")).isEmpty();

            DesiredState slow = new DesiredState("slow", RUNNING, "slow-rev");
            desired.save(slow);
            assertThat(work.offer("slow", slow, () -> loop.converge("slow")))
                    .isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);
            assertThat(slowEntered.await(5, TimeUnit.SECONDS)).isTrue();
            if (change == PendingChange.REBUILD) {
                desired.save(new DesiredState("orders", RUNNING, original.revision(), original.purgeState(),
                        original.assemblyRevision(), true, failed.epoch()));
            } else if (change == PendingChange.INCARNATION) {
                scopes.forgetIncarnation("orders", "inc-a");
                assertThat(scopes.current("orders")).isEmpty();
                scopes.forgetIncarnation("orders", "inc-a");
                state.delete("orders");
                state.create("orders", StateJson.of(NEW), T0);
                scopes.begin("orders", "inc-b", 2);
            } else {
                recover.set(true);
            }

            isolated.reconcile();

            if (change == PendingChange.RECOVERY) {
                releaseSlow.countDown();
                assertThat(preparingRecovery.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(admissions).as("pending classification cannot consume a second recovery admission")
                        .hasValue(1);
                assertThat(desired.read("orders")).contains(original);
                assertThat(starts).hasValue(0);
            }
            assertThat(pending.pending("orders").orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            releaseSlow.countDown();
            releaseRecovery.countDown();
        } finally {
            releaseSlow.countDown();
            releaseRecovery.countDown();
        }
    }

    @Test
    void aMissingPausedJobReachesTheStoreBackedStatusWithItsCodedReason() {
        AtomicBoolean carrying = new AtomicBoolean(true);
        LifecycleActuator job = new LifecycleActuator() {
            @Override public void start(String id) { carrying.set(true); }
            @Override public void pause(String id) { }
            @Override public void resume(String id) { }
            @Override public void stop(String id, boolean purgeState) { carrying.set(false); }
            @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
            @Override public Optional<Throwable> lost(String id) { return Optional.empty(); }
            @Override public boolean isCarryingAJob(String id) { return carrying.get(); }
        };
        PipelineConverger loop = new PipelineConverger(desired, state, job, Clock.fixed(T0, ZoneOffset.UTC));
        ConvergenceDriver observed = new ConvergenceDriver(
                loop, desired, new ObservationPublisher(state, observations));
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        observed.reconcile();
        desired.save(new DesiredState("orders", io.tapstate.core.lifecycle.PipelineState.PAUSED, "rev-1"));
        observed.reconcile();
        carrying.set(false);

        observed.reconcile();

        Observation latest = observations.read("orders").orElseThrow();
        assertThat(latest.state()).isEqualTo(FAILED);
        assertThat(latest.failure().code()).isEqualTo("lifecycle.paused-job-missing");
        assertThat(desired.read("orders").orElseThrow().targetState())
                .isEqualTo(io.tapstate.core.lifecycle.PipelineState.PAUSED);
    }

    private static final Instant T0 = Instant.parse("2026-07-01T00:00:00Z");

    private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
    private final InMemoryStateStore state = new InMemoryStateStore();
    private final InMemoryObservationStore observations = new InMemoryObservationStore();
    private final PipelineConverger converger =
            new PipelineConverger(desired, state, new NoOpActuator(), Clock.fixed(T0, ZoneOffset.UTC));
    private final ConvergenceDriver driver =
            new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));

    @Test
    void reconcileConvergesEveryDesiredPipeline() {
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        desired.save(new DesiredState("users", RUNNING, "rev-1"));

        driver.reconcile();

        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(state.read("users").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        // Each converged pipeline also has its observation published for the read faces to serve.
        assertThat(observations.read("orders").orElseThrow().state()).isEqualTo(RUNNING);
        assertThat(observations.read("users").orElseThrow().state()).isEqualTo(RUNNING);
    }

    @Test
    void aBlockedLifecycleStartDoesNotStopAnotherPipelinesObservation() throws Exception {
        CountDownLatch slowEntered = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        CountDownLatch fastEntered = new CountDownLatch(1);
        LifecycleActuator blocking = new LifecycleActuator() {
            @Override
            public void start(String pipelineId) {
                if (pipelineId.equals("slow")) {
                    slowEntered.countDown();
                    try {
                        if (!releaseSlow.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("slow start was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("slow start was interrupted", interrupted);
                    }
                } else {
                    fastEntered.countDown();
                }
            }

            @Override public void pause(String pipelineId) { }
            @Override public void resume(String pipelineId) { }
            @Override public void stop(String pipelineId, boolean purgeState) { }
            @Override public Optional<Throwable> failure(String pipelineId) { return Optional.empty(); }
            @Override public Optional<Throwable> lost(String pipelineId) { return Optional.empty(); }
            @Override public boolean isCarryingAJob(String pipelineId) { return true; }
        };
        desired.save(new DesiredState("slow", RUNNING, "rev-1"));
        desired.save(new DesiredState("fast", RUNNING, "rev-1"));
        PipelineConverger asyncConverger = new PipelineConverger(
                desired, state, blocking, Clock.fixed(T0, ZoneOffset.UTC));
        try (LifecycleWorkDispatcher work = new LifecycleWorkDispatcher(2, 2)) {
            ConvergenceDriver async = new ConvergenceDriver(asyncConverger, desired,
                    new ObservationPublisher(state, observations), (RateSampler) null,
                    MetricsExport.none(), () -> true, PipelineActuationOwnership.single(), work);

            async.reconcile();
            assertThat(slowEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(fastEntered.await(5, TimeUnit.SECONDS))
                    .as("the fast pipeline starts while the slow snapshot is still blocked").isTrue();
            assertThat(releaseSlow.getCount()).isEqualTo(1L);
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < until && observations.read("fast")
                    .filter(observation -> observation.state() == RUNNING).isEmpty()) {
                async.reconcile();
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(observations.read("fast").orElseThrow().state()).isEqualTo(RUNNING);
            assertThat(releaseSlow.getCount()).isEqualTo(1L);
        } finally {
            releaseSlow.countDown();
        }
    }

    @Test
    void aStalledObservationWriteDoesNotHoldTheNextPipelinesConvergence() throws Exception {
        desired.save(new DesiredState("slow", RUNNING, "rev-1"));
        desired.save(new DesiredState("fast", RUNNING, "rev-1"));
        CountDownLatch slowWriteEntered = new CountDownLatch(1);
        CountDownLatch releaseSlowWrite = new CountDownLatch(1);
        ObservationStore blocked = new ObservationStore() {
            @Override
            public void save(Observation observation) {
                if (observation.pipelineId().equals("slow")) {
                    slowWriteEntered.countDown();
                    try {
                        if (!releaseSlowWrite.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("slow observation write was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("observation write was interrupted", interrupted);
                    }
                }
                observations.save(observation);
            }

            @Override public Optional<Observation> read(String pipelineId) {
                return observations.read(pipelineId);
            }

            @Override public void delete(String pipelineId) {
                observations.delete(pipelineId);
            }
        };
        ObservationPublisher writer = new ObservationPublisher(state, blocked);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (TelemetryDispatcher telemetry = new TelemetryDispatcher(writer, null, MetricsExport.none(), 2, 2)) {
            ConvergenceDriver isolated = new ConvergenceDriver(
                    converger, desired, writer, null, MetricsExport.none(), () -> true,
                    PipelineActuationOwnership.single(), LifecycleWorkDispatcher.inline(), null, telemetry);
            Future<?> pass = caller.submit(isolated::reconcile);
            assertThat(slowWriteEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(releaseSlowWrite.getCount()).isEqualTo(1L);
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while ((state.read("fast").isEmpty() || observations.read("fast").isEmpty())
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(state.read("fast")).as("the fast pipeline converges while telemetry is stalled")
                    .isPresent();
            assertThat(observations.read("fast")).as("the fast pipeline remains observable").isPresent();
            releaseSlowWrite.countDown();
            pass.get(5, TimeUnit.SECONDS);
        } finally {
            releaseSlowWrite.countDown();
            caller.shutdownNow();
            assertThat(caller.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void aStalledStoredCountDoesNotHoldTheNextPipelinesConvergence() throws Exception {
        desired.save(new DesiredState("slow", RUNNING, "rev-1"));
        desired.save(new DesiredState("fast", RUNNING, "rev-1"));
        CountDownLatch countEntered = new CountDownLatch(1);
        CountDownLatch releaseCount = new CountDownLatch(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (StoredCountSampler sampler = new StoredCountSampler((database, namespace) -> {
            countEntered.countDown();
            try {
                if (!releaseCount.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("stored count was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
            return 10;
        }, Clock.systemUTC())) {
            ObservationPublisher writer = new ObservationPublisher(state, observations,
                    id -> OptionalLong.empty(), id -> Map.of(), id -> SnapshotReading.NONE,
                    id -> Map.of(), id -> {
                        if (!id.equals("slow")) {
                            return Map.of();
                        }
                        var stored = sampler.sample(id, 1L, "db", "ns");
                        return Map.of("ns", new NestStateReading(1, 100, 0, 0, 0,
                                stored.map(value -> OptionalLong.of(value.value())).orElseGet(OptionalLong::empty),
                                stored.map(StoredCountSampler.Sample::observedAt)));
                    });
            try (TelemetryDispatcher telemetry = new TelemetryDispatcher(
                    writer, null, MetricsExport.none(), 2, 2)) {
                ConvergenceDriver isolated = new ConvergenceDriver(converger, desired, writer, null,
                        MetricsExport.none(), () -> true, PipelineActuationOwnership.single(),
                        LifecycleWorkDispatcher.inline(), null, telemetry);
                Future<?> pass = caller.submit(isolated::reconcile);
                assertThat(countEntered.await(5, TimeUnit.SECONDS)).isTrue();
                pass.get(1, TimeUnit.SECONDS);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while ((observations.read("fast").isEmpty() || observations.read("slow").isEmpty())
                        && System.nanoTime() < deadline) {
                    TimeUnit.MILLISECONDS.sleep(5);
                }
                assertThat(state.read("fast")).isPresent();
                assertThat(observations.read("fast")).isPresent();
                assertThat(observations.read("slow").orElseThrow().metrics())
                        .doesNotContainKey("nestStateStored.ns");
            }
        } finally {
            releaseCount.countDown();
            caller.shutdownNow();
        }
    }

    @Test
    void aMemberWithoutCommittedMembershipDoesNotActuateDesiredWork() {
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        ConvergenceDriver gated = new ConvergenceDriver(
                converger, desired, new ObservationPublisher(state, observations), () -> false);

        gated.reconcile();

        assertThat(state.read("orders")).isEmpty();
        assertThat(observations.read("orders")).isEmpty();
    }

    @Test
    void reconcileIsolatesAPerPipelineFailure() {
        desired.save(new DesiredState("broken", RUNNING, "rev-1"));
        desired.save(new DesiredState("healthy", RUNNING, "rev-1"));
        state.failFor("broken");

        driver.reconcile();

        // The healthy pipeline still converges even though the broken one threw mid-pass.
        assertThat(state.read("healthy").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
    }

    @Test
    void aPipelineWhoseReconcileKeepsThrowingBecomesObservableWithAClimbingErrorCount() {
        // A pipeline whose converge pass throws every tick (its store is unreachable) never reaches the
        // publish call, so with no error observation it is indistinguishable from one that is merely slow to
        // converge. The driver must count the consecutive failures and surface them, so "permanently broken"
        // reads differently from "still converging" on the observation read face.
        desired.save(new DesiredState("broken", RUNNING, "rev-1"));
        state.failFor("broken");

        driver.reconcile();
        driver.reconcile();
        driver.reconcile();

        Observation observed = observations.read("broken").orElseThrow();
        // Never converged, so no lifecycle state was ever witnessed: the projection stays NEW rather than a
        // fabricated FAILED, and the climbing streak is what marks it broken. It is published as a streak
        // and named as one -- a pass that keeps throwing never reaches a publish, so there is no witnessed
        // failure with a code to count, only the fact that the attempt is not getting through.
        assertThat(observed.state()).isEqualTo(NEW);
        assertThat(observed.metrics()).containsOnly(entry("reconcileFailuresInARow", 3L));
    }

    @Test
    void theReconcileFailureStreakClearsThenRestartsAfterThePipelineRecovers() {
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        state.failFor("orders");
        driver.reconcile();
        driver.reconcile();
        assertThat(observations.read("orders").orElseThrow().metrics())
                .containsOnly(entry("reconcileFailuresInARow", 2L));

        // The store recovers: the next pass converges normally and republishes the real state, so the
        // streak stops being published rather than staying stuck at the earlier total. Absent and not
        // nought: there is no streak running, and a nought would say there is one that has just reset.
        state.recover("orders");
        driver.reconcile();
        Observation recovered = observations.read("orders").orElseThrow();
        assertThat(recovered.state()).isEqualTo(RUNNING);
        assertThat(recovered.metrics()).isEmpty();

        // A later failure starts a fresh streak from one, not from the earlier total of two, so the clean pass
        // in between must have cleared the counter.
        state.failFor("orders");
        driver.reconcile();
        assertThat(observations.read("orders").orElseThrow().metrics())
                .containsOnly(entry("reconcileFailuresInARow", 1L));
    }

    @Test
    void aFailingPipelineDroppedFromTheDesiredSetDoesNotKeepItsStreak() {
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        state.failFor("orders");
        driver.reconcile();
        driver.reconcile(); // the streak climbs to two

        // The pipeline is deleted while still failing; the pass that no longer sees it prunes its counter so a
        // deleted-while-failing pipeline cannot leak a streak that nothing will ever clear.
        desired.remove("orders");
        driver.reconcile();

        // Re-applied later and still failing, it starts a fresh streak from one rather than resuming at three.
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        driver.reconcile();
        assertThat(observations.read("orders").orElseThrow().metrics())
                .containsOnly(entry("reconcileFailuresInARow", 1L));
    }

    @Test
    void aReadFaceThatRejectsTheErrorObservationDoesNotStarveTheOtherPipelines() {
        // The broken pipeline's converge keeps throwing and the read face also rejects its error observation
        // (the same store backs both and is down). Publishing the failure is best-effort: it must be swallowed
        // so a second, healthy pipeline still converges and is observed in the same pass. Without that, one
        // unreachable read face would starve every pipeline reconciled after it.
        desired.save(new DesiredState("broken", RUNNING, "rev-1"));
        desired.save(new DesiredState("healthy", RUNNING, "rev-1"));
        state.failFor("broken");
        observations.failSaveFor("broken");

        driver.reconcile(); // must return normally rather than propagating the rejected save

        assertThat(observations.read("healthy").orElseThrow().state()).isEqualTo(RUNNING);
        // The broken pipeline's error observation could not be written, but that did not abort the pass.
        assertThat(observations.read("broken")).isEmpty();
    }

    @Test
    void logsEmittedDuringAPipelinesPassAreAttributedToThatPipeline() {
        // Feed the node-local sink from the driver's own logger via the pipeline appender; make one pipeline
        // fail so the driver logs a warning during that pipeline's turn. The warning must land under that
        // pipeline id, proving the driver sets the pipeline attribution slot around each pipeline's work.
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        Logger driverLogger = (Logger) LoggerFactory.getLogger(ConvergenceDriver.class);
        PipelineLogAppender appender = new PipelineLogAppender(sink, new io.tapstate.core.logging.SecretRedactor());
        appender.setContext(driverLogger.getLoggerContext());
        appender.start();
        driverLogger.addAppender(appender);
        try {
            desired.save(new DesiredState("broken", RUNNING, "rev-1"));
            state.failFor("broken");

            driver.reconcile();
        } finally {
            driverLogger.detachAppender(appender);
            appender.stop();
        }

        assertThat(sink.tail("broken")).extracting(LogLine::level).containsExactly("WARN");
        // The attribution slot is cleared after the pass, so an unrelated later log is not misattributed.
        assertThat(MDC.get("pipeline_id")).isNull();
    }

    @Test
    void aColdRestoredFailureLogsOnceUnderItsQualifiedScope() throws Exception {
        requireColdFailureLog(false);
    }

    @Test
    void aScopeRestoredBetweenInitialLogBindingAndPublicationStillScopesTheFailure() throws Exception {
        requireColdFailureLog(true);
    }

    @Test
    void aColdFailureOfferedAfterScopeRegistrationKeepsItsCauseAndScopedWarning() throws Exception {
        requireColdFailureLog(false, true);
    }

    @Test
    void aFailureJoiningAnAlreadyPreparedColdTicketKeepsItsCauseAndScopedWarning() throws Exception {
        requireColdFailureLog(false, false, true);
    }

    private void requireColdFailureLog(boolean restoreDuringConvergence) throws Exception {
        requireColdFailureLog(restoreDuringConvergence, false);
    }

    private void requireColdFailureLog(boolean restoreDuringConvergence, boolean restoreDuringOffer) throws Exception {
        requireColdFailureLog(restoreDuringConvergence, restoreDuringOffer, false);
    }

    private void requireColdFailureLog(boolean restoreDuringConvergence, boolean restoreDuringOffer,
            boolean failureAfterNeutralPreparation) throws Exception {
        String pipeline = "orders";
        String cluster = "cluster-a";
        var generations = new InMemoryWorkloadClaimStore();
        long generation = generations.advanceStandalone(cluster, pipeline).orElseThrow();
        var resources = new InMemoryArtifactStore();
        resources.save(new io.tapstate.core.model.PipelineResource(pipeline, null, List.of(),
                null, null, null, null, null));
        var incarnation = new java.util.concurrent.atomic.AtomicReference<String>();
        io.tapstate.spi.store.ArtifactStore artifacts = new io.tapstate.spi.store.ArtifactStore() {
            @Override public void saveAll(List<io.tapstate.core.model.Resource> values) { resources.saveAll(values); }
            @Override public Optional<io.tapstate.core.model.Resource> get(String id) { return resources.get(id); }
            @Override public List<io.tapstate.core.model.Resource> list() { return resources.list(); }
            @Override public Optional<String> pipelineIncarnationId(String id) {
                return pipeline.equals(id) ? Optional.ofNullable(incarnation.get()) : Optional.empty();
            }
            @Override public Optional<String> ensurePipelineIncarnationId(String id, String candidate) {
                if (!pipeline.equals(id) || resources.get(id).isEmpty()) { return Optional.empty(); }
                incarnation.compareAndSet(null, candidate);
                return Optional.of(incarnation.get());
            }
        };
        var scope = new ObservationStore.Scope(artifacts.ensurePipelineIncarnationId(
                pipeline, java.util.UUID.randomUUID().toString()).orElseThrow(), generation);
        var stored = new java.util.concurrent.atomic.AtomicReference<ObservationStore.Stored>();
        CountDownLatch coldReadEntered = new CountDownLatch(1);
        CountDownLatch releaseColdRead = new CountDownLatch(1);
        CountDownLatch neutralSaveEntered = new CountDownLatch(1);
        CountDownLatch releaseNeutralSave = new CountDownLatch(1);
        AtomicBoolean blockNeutralSave = new AtomicBoolean(failureAfterNeutralPreparation);
        AtomicBoolean blockFirstColdRead = new AtomicBoolean(true);
        var coldWorkerContext = new java.util.concurrent.atomic.AtomicReference<PipelineLogContext>();
        var idleWorkerContext = new java.util.concurrent.atomic.AtomicReference<PipelineLogContext>();
        AtomicBoolean probeColdContext = new AtomicBoolean();
        List<PipelineLogContext> contextsAtWorkerWrites = new CopyOnWriteArrayList<>();
        ObservationStore latest = new ObservationStore() {
            @Override public void save(Observation observation) { throw new AssertionError("unscoped write"); }
            @Override public boolean saveScoped(Observation observation, Scope owner) {
                if (Thread.currentThread().getName().startsWith("tapstate-telemetry-latest-")) {
                    contextsAtWorkerWrites.add(PipelineLogContext.capture());
                    if (blockNeutralSave.compareAndSet(true, false)) {
                        assertThat(observation.state()).as("the neutral worker prepared the real FAILED checkpoint")
                                .isEqualTo(FAILED);
                        assertThat(observation.failure()).as("the cause has not joined the cold ticket yet").isNull();
                        neutralSaveEntered.countDown();
                        try {
                            if (!releaseNeutralSave.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("prepared neutral cold save was not released");
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(interrupted);
                        }
                    }
                }
                stored.set(new Stored(observation, Optional.of(owner)));
                return true;
            }
            @Override public Optional<Observation> read(String id) {
                return readStored(id).map(Stored::observation);
            }
            @Override public Optional<Stored> readStored(String id) {
                if (!pipeline.equals(id)) { return Optional.empty(); }
                if (Thread.currentThread().getName().startsWith("tapstate-telemetry-latest-")
                        && probeColdContext.compareAndSet(true, false)) {
                    idleWorkerContext.set(PipelineLogContext.capture());
                }
                if (Thread.currentThread().getName().startsWith("tapstate-telemetry-latest-")
                        && blockFirstColdRead.compareAndSet(true, false)) {
                    coldWorkerContext.set(PipelineLogContext.capture());
                    coldReadEntered.countDown();
                    try {
                        if (!releaseColdRead.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("cold identity read was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    }
                }
                return Optional.ofNullable(stored.get());
            }
            @Override public void delete(String id) { stored.set(null); }
        };
        desired.save(new DesiredState(pipeline, RUNNING, "rev-1"));
        state.create(pipeline, StateJson.of(RUNNING), T0);
        new ObservationPublisher(state, latest).publishScoped(pipeline, null, scope).orElseThrow();
        ObservationScopeRegistry scopes = restoreDuringOffer || failureAfterNeutralPreparation
                ? org.mockito.Mockito.spy(new ObservationScopeRegistry()) : new ObservationScopeRegistry();
        assertThat(scopes.current(pipeline)).isEmpty();
        if (restoreDuringOffer || failureAfterNeutralPreparation) {
            org.mockito.Mockito.doAnswer(invocation -> {
                releaseColdRead.countDown();
                if (failureAfterNeutralPreparation) {
                    // The real worker cached its neutral preparation and entered its scoped save before this cause.
                    assertThat(neutralSaveEntered.await(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(scopes.current(pipeline)).isEmpty();
                    var ticket = org.mockito.ArgumentCaptor.forClass(ObservationScopeRegistry.RestoreTicket.class);
                    var qualified = org.mockito.ArgumentCaptor.forClass(ObservationScopeRecovery.Qualified.class);
                    var prepared = org.mockito.ArgumentCaptor.forClass(ObservationPublisher.Prepared.class);
                    org.mockito.Mockito.verify(scopes).rememberRestoration(
                            ticket.capture(), qualified.capture(), prepared.capture());
                    assertThat(qualified.getValue().scope()).isEqualTo(scope);
                    var failed = ((io.tapstate.runtime.scheduler.ConvergeResult) invocation.getArgument(1))
                            .checkpoint().orElseThrow();
                    assertThat(qualified.getValue().checkpoint()).isEqualTo(failed);
                    assertThat(scopes.restorationPrepared(ticket.getValue(), qualified.getValue()))
                            .as("the actual neutral frame was cached before the driver admitted its cause")
                            .contains(prepared.getValue());
                    assertThat(prepared.getValue().observation().failure()).isNull();
                    Optional<?> admitted = (Optional<?>) invocation.callRealMethod();
                    assertThat(admitted).as("the real cause joins while the existing cold ticket is still pending")
                            .isPresent();
                    releaseNeutralSave.countDown();
                    return admitted;
                }
                // The driver already read an absent scope. Finish the actual neutral recovery before ticket admission.
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (scopes.current(pipeline).isEmpty() && System.nanoTime() - until < 0) {
                    TimeUnit.MILLISECONDS.sleep(5);
                }
                assertThat(scopes.current(pipeline)).contains(scope);
                return invocation.callRealMethod();
            }).when(scopes).restoration(org.mockito.ArgumentMatchers.eq(pipeline),
                    org.mockito.ArgumentMatchers.any(io.tapstate.runtime.scheduler.ConvergeResult.class),
                    org.mockito.ArgumentMatchers.any(ObservationFailure.class),
                    org.mockito.ArgumentMatchers.nullable(ObservationScopeRecovery.Owner.class),
                    org.mockito.ArgumentMatchers.any(TelemetryDispatcher.FailureLog.class));
        }
        ObservationPublisher publisher = new ObservationPublisher(state, latest);
        var recovery = new ObservationScopeRecovery(artifacts, generations, latest, state, cluster);
        FailingActuator actuator = new FailingActuator();
        var cause = new io.tapstate.core.common.TapstateException(io.tapstate.runtime.engine.EngineError.JOB_FAILED,
                Map.of("pipeline", pipeline, "cause", "cold sink failure"), new java.io.IOException("native cause"));
        actuator.failWith(cause);
        LifecycleActuator failureGate = new LifecycleActuator() {
            @Override public void start(String id) { actuator.start(id); }
            @Override public void pause(String id) { actuator.pause(id); }
            @Override public void resume(String id) { actuator.resume(id); }
            @Override public void stop(String id, boolean purgeState) { actuator.stop(id, purgeState); }
            @Override public Optional<Throwable> failure(String id) {
                if (restoreDuringConvergence) {
                    // The tick already bound its absent scope. Resolve the real stored owner before it publishes.
                    releaseColdRead.countDown();
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (scopes.current(pipeline).isEmpty() && System.nanoTime() - until < 0) {
                        try { TimeUnit.MILLISECONDS.sleep(5); }
                        catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(interrupted);
                        }
                    }
                    assertThat(scopes.current(pipeline)).contains(scope);
                }
                return actuator.failure(id);
            }
            @Override public Optional<Throwable> lost(String id) { return actuator.lost(id); }
            @Override public boolean isCarryingAJob(String id) { return actuator.isCarryingAJob(id); }
        };
        var loop = new PipelineConverger(desired, state, failureGate, Clock.fixed(T0.plusSeconds(1), ZoneOffset.UTC));
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        Logger driverLogger = (Logger) LoggerFactory.getLogger(ConvergenceDriver.class);
        Logger telemetryLogger = (Logger) LoggerFactory.getLogger(TelemetryDispatcher.class);
        PipelineLogAppender appender = new PipelineLogAppender(sink, new io.tapstate.core.logging.SecretRedactor());
        appender.setContext(driverLogger.getLoggerContext());
        appender.start();
        driverLogger.addAppender(appender);
        telemetryLogger.addAppender(appender);
        PipelineLogContext original = PipelineLogContext.capture();
        MDC.put(io.tapstate.core.logging.PipelineAttribution.MDC_KEY, "caller");
        MDC.put(io.tapstate.core.logging.PipelineAttribution.INCARNATION_MDC_KEY, "caller-incarnation");
        MDC.put(io.tapstate.core.logging.PipelineAttribution.EXECUTION_MDC_KEY, "1");
        PipelineLogContext caller = PipelineLogContext.capture();
        try (TelemetryDispatcher telemetry = new TelemetryDispatcher(publisher, null, MetricsExport.none(),
                scopes, null, recovery, 1, 4)) {
            var restoredDriver = new ConvergenceDriver(loop, desired, publisher, null, MetricsExport.none(),
                    () -> true, PipelineActuationOwnership.single(), LifecycleWorkDispatcher.inline(), scopes, telemetry);
            if (restoreDuringConvergence || restoreDuringOffer || failureAfterNeutralPreparation) {
                telemetry.offerScopeRecovery(pipeline, null, null, () -> true);
                assertThat(coldReadEntered.await(5, TimeUnit.SECONDS)).isTrue();
            }
            restoredDriver.reconcile();
            assertThat(coldReadEntered.await(5, TimeUnit.SECONDS)).isTrue();
            if (!restoreDuringConvergence && !restoreDuringOffer && !failureAfterNeutralPreparation) {
                assertThat(scopes.current(pipeline)).as("cold store reads stay off convergence").isEmpty();
            }
            assertThat(state.read(pipeline).orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
            assertThat(PipelineLogContext.capture()).as("convergence restores the caller MDC").isEqualTo(caller);
            releaseColdRead.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while ((scopes.current(pipeline).isEmpty()
                    || stored.get().observation().state() != FAILED
                    || stored.get().observation().failure() == null
                    || telemetry.health().get(TelemetryDispatcher.Sink.LATEST).successes() == 0
                    || telemetry.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() != 0)
                    && System.nanoTime() - deadline < 0) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(scopes.current(pipeline)).contains(scope);
            assertThat(stored.get().scope()).contains(scope);
            assertThat(stored.get().observation().state()).isEqualTo(FAILED);
            assertThat(stored.get().observation().failure())
                    .as("the real FAILED result retains its original cause through cold handoff").isNotNull();
            assertThat(stored.get().observation().failure().code()).isEqualTo(cause.code().code());
            assertThat(stored.get().observation().failure().params())
                    .containsEntry("cause", "cold sink failure");
            assertThat(stored.get().observation().metrics()).containsEntry("errors." + cause.code().code(), 1L);
            long published = telemetry.health().get(TelemetryDispatcher.Sink.LATEST).successes();
            restoredDriver.reconcile();
            restoredDriver.reconcile();
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while ((telemetry.health().get(TelemetryDispatcher.Sink.LATEST).successes() <= published
                    || telemetry.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() != 0
                    || telemetry.health().get(TelemetryDispatcher.Sink.LATEST).queueDepth() != 0)
                    && System.nanoTime() - deadline < 0) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(telemetry.health().get(TelemetryDispatcher.Sink.LATEST).successes()).isGreaterThan(published);
            var logScope = new io.tapstate.core.logging.LogSink.Scope(scope.pipelineIncarnationId(), generation);
            List<LogLine> failureLines = sink.tail(pipeline, logScope).stream()
                    .filter(line -> "WARN".equals(line.level()) && line.message().contains("entered FAILED"))
                    .toList();
            assertThat(failureLines).as("one actual cold failure belongs to the restored execution; legacy=%s",
                    sink.tail(pipeline)).hasSize(1);
            assertThat(failureLines.getFirst().message()).contains(cause.code().code(), "cold sink failure",
                    "TapstateException", "java.io.IOException: native cause");
            assertThat(sink.tail(pipeline)).as("an identified cold failure is never attributed as legacy").isEmpty();
            assertThat(contextsAtWorkerWrites).isNotEmpty();
            assertThat(contextsAtWorkerWrites.getFirst()).as("the initial cold commit restores its entry MDC")
                    .isEqualTo(coldWorkerContext.get());
            PipelineLogContext scopedPreparation = new PipelineLogContext(
                    pipeline, scope.pipelineIncarnationId(), Long.toString(generation));
            if (!failureAfterNeutralPreparation) {
                assertThat(contextsAtWorkerWrites.subList(1, contextsAtWorkerWrites.size()))
                        .as("normal warm preparation deliberately scopes its complete save")
                        .isNotEmpty().containsOnly(scopedPreparation);
            } else {
                // A legitimate cold retry can precede the ordinary scoped ticks in this ordering.
                assertThat(contextsAtWorkerWrites).contains(scopedPreparation);
            }
            assertThat(PipelineLogContext.capture()).as("later ticks restore the caller MDC").isEqualTo(caller);

            // Reuse the same idle worker for a fresh cold request after every warm wrapper has exited.
            scopes.forgetIncarnation(pipeline, scope.pipelineIncarnationId());
            probeColdContext.set(true);
            long beforeProbe = telemetry.health().get(TelemetryDispatcher.Sink.LATEST).successes();
            telemetry.offerScopeRecovery(pipeline, null, null, () -> true);
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while ((scopes.current(pipeline).isEmpty()
                    || telemetry.health().get(TelemetryDispatcher.Sink.LATEST).successes() <= beforeProbe
                    || telemetry.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() != 0)
                    && System.nanoTime() - deadline < 0) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(scopes.current(pipeline)).contains(scope);
            assertThat(idleWorkerContext.get()).as("warm preparation leaves no MDC on the reused cold worker")
                    .isEqualTo(coldWorkerContext.get());
            assertThat(contextsAtWorkerWrites.getLast()).as("the fresh cold commit preserves its entry MDC")
                    .isEqualTo(coldWorkerContext.get());
            assertThat(sink.tail(pipeline, logScope).stream().filter(line -> "WARN".equals(line.level())
                    && line.message().contains("entered FAILED"))).hasSize(1);
            assertThat(generations.currentGeneration(cluster, pipeline)).hasValue(generation);
            assertThat(stored.get().observation().metrics()).containsEntry("errors." + cause.code().code(), 1L);
        } finally {
            releaseColdRead.countDown();
            releaseNeutralSave.countDown();
            original.restore();
            driverLogger.detachAppender(appender);
            telemetryLogger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void aPipelineWhoseJobDiedIsDrivenToFailedAndLogged() {
        // A job that dies on its own does not throw into the reconcile pass — the converge side moves the
        // pipeline to FAILED and returns it. The driver must log that, so a dead job is no longer the
        // silent "found 0" it used to be, and the observable state reflects the failure.
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        Logger driverLogger = (Logger) LoggerFactory.getLogger(ConvergenceDriver.class);
        PipelineLogAppender appender = new PipelineLogAppender(sink, new io.tapstate.core.logging.SecretRedactor());
        appender.setContext(driverLogger.getLoggerContext());
        appender.start();
        driverLogger.addAppender(appender);

        FailingActuator actuator = new FailingActuator();
        PipelineConverger converger =
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));
        ConvergenceDriver driver =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        try {
            desired.save(new DesiredState("orders", RUNNING, "rev-1"));
            driver.reconcile(); // orders -> RUNNING while the job is healthy
            actuator.failWith(new RuntimeException("sink write failed"));

            driver.reconcile(); // the job has died: orders -> FAILED, and the driver logs it
        } finally {
            driverLogger.detachAppender(appender);
            appender.stop();
        }

        assertThat(sink.tail("orders")).extracting(LogLine::level).contains("WARN");
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
    }

    @Test
    void aPipelineWhoseJobDiedPublishesTheCodedReasonAlongsideTheFailedState() {
        // The state says the run died and the error count says it was counted; neither says why. The driver
        // holds the only copy of the cause, so it must carry it into the published observation -- otherwise
        // the reason exists solely as a log line and no read face can answer for it.
        FailingActuator actuator = new FailingActuator();
        PipelineConverger converger =
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));
        ConvergenceDriver driver =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        driver.reconcile();
        actuator.failWith(new IllegalStateException("sink write failed"));

        driver.reconcile();

        ObservationFailure failure = observations.read("orders").orElseThrow().failure();
        assertThat(failure).isNotNull();
        assertThat(failure.code()).isEqualTo("engine.job-failed");
        assertThat(failure.params()).containsEntry("cause", "sink write failed");
    }

    @Test
    void aPipelineStillFailedOnALaterPassKeepsReportingWhyItDied() {
        // PipelineConverger reports the cause only on the one pass that drives RUNNING -> FAILED; every
        // later pass over an already-FAILED checkpoint returns a bare CONVERGED with no cause attached (it
        // did not just fail again, it is still failed from before). Without the driver carrying it forward,
        // a pipeline dead for an hour would say why for exactly the one second it transitioned in.
        FailingActuator actuator = new FailingActuator();
        PipelineConverger converger =
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));
        ConvergenceDriver driver =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        driver.reconcile();
        actuator.failWith(new IllegalStateException("sink write failed"));
        driver.reconcile(); // orders -> FAILED, cause published

        driver.reconcile(); // still FAILED, unchanged
        driver.reconcile(); // still FAILED, unchanged again

        ObservationFailure failure = observations.read("orders").orElseThrow().failure();
        assertThat(failure).isNotNull();
        assertThat(failure.code()).isEqualTo("engine.job-failed");
        assertThat(failure.params()).containsEntry("cause", "sink write failed");
        // One death is one count, driven through the real converger rather than asserted against a
        // publisher fed by hand: the cause arrives on one pass and the three after it carry none, so a
        // count taken from the state instead of from the witness would read four here.
        assertThat(observations.read("orders").orElseThrow().metrics())
                .containsOnly(entry("errors.engine.job-failed", 1L));
    }

    @Test
    void aDeletedPipelinesFailureCountsGoWithIt() {
        // The sweep through its actual trigger. Nothing else can witness this: the publisher's own case
        // calls the method directly, so a sweep that existed and was never called would pass it -- and a
        // count kept per pipeline id, never cleared, grows with every pipeline the process ever saw.
        FailingActuator actuator = new FailingActuator();
        PipelineConverger converger =
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));
        ConvergenceDriver driver =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        driver.reconcile();
        actuator.failWith(new IllegalStateException("sink write failed"));
        driver.reconcile(); // orders -> FAILED, one failure counted
        assertThat(observations.read("orders").orElseThrow().metrics())
                .containsOnly(entry("errors.engine.job-failed", 1L));

        // Deleted: its intent is removed, which is the only thing that takes a pipeline out of the set the
        // pass reads. A stopped pipeline would still be in it and would keep its counts.
        desired.remove("orders");
        driver.reconcile();

        // Re-applied under the same id. Its checkpoint is still FAILED, so this pass converges with no
        // cause to report and publishes whatever the account holds -- nothing, if the delete was seen.
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        driver.reconcile();
        assertThat(observations.read("orders").orElseThrow().metrics()).isEmpty();
    }

    @Test
    void aPipelineStillFailedAfterARestartKeepsReportingWhyItDied() {
        // A restart rebuilds the driver but not the stores: the FAILED checkpoint and its published reason
        // both survive in durable state, so the first pass of the new process must keep answering with them.
        // Anything the old driver only held in memory is gone here -- if the reason rode only there, this
        // pass would republish the still-FAILED pipeline reasonless, the exact erasure the carry-forward
        // exists to prevent.
        FailingActuator actuator = new FailingActuator();
        PipelineConverger converger =
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));
        ConvergenceDriver driver =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        driver.reconcile();
        actuator.failWith(new IllegalStateException("sink write failed"));
        driver.reconcile(); // orders -> FAILED, cause published
        assertThat(observations.read("orders").orElseThrow().failure()).isNotNull();

        ConvergenceDriver restarted =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        restarted.reconcile();

        ObservationFailure failure = observations.read("orders").orElseThrow().failure();
        assertThat(failure).isNotNull();
        assertThat(failure.code()).isEqualTo("engine.job-failed");
        assertThat(failure.params()).containsEntry("cause", "sink write failed");
    }

    @Test
    void aPipelineThatRecoversStopsReportingTheFailureThatKilledItsPreviousRun() {
        FailingActuator actuator = new FailingActuator();
        PipelineConverger converger =
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));
        ConvergenceDriver driver =
                new ConvergenceDriver(converger, desired, new ObservationPublisher(state, observations));
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        driver.reconcile();
        actuator.failWith(new IllegalStateException("sink write failed"));
        driver.reconcile();
        assertThat(observations.read("orders").orElseThrow().failure()).isNotNull();

        // A failed run stays failed on its own (PipelineConverger never re-drives FAILED toward RUNNING);
        // the only real recovery path is an explicit stop, then a fresh start. Once that happens the
        // observation is current-state again, so the stale reason must not keep being served.
        actuator.failWith(null);
        desired.save(new DesiredState("orders", STOPPED, "rev-2"));
        driver.reconcile();
        desired.save(new DesiredState("orders", RUNNING, "rev-3"));
        driver.reconcile();

        Observation recovered = observations.read("orders").orElseThrow();
        assertThat(recovered.state()).isEqualTo(RUNNING);
        assertThat(recovered.failure()).isNull();
    }

    /** A no-op actuator whose failure() a test can arm, to drive a pipeline to FAILED without a real job. */
    private static final class FailingActuator implements LifecycleActuator {
        private Throwable failure;

        void failWith(Throwable cause) {
            this.failure = cause;
        }

        @Override
        public void start(String pipelineId) {
        }

        @Override
        public void pause(String pipelineId) {
        }

        @Override
        public void resume(String pipelineId) {
        }

        @Override
        public void stop(String pipelineId, boolean purgeState) {
        }

        @Override
        public Optional<Throwable> failure(String pipelineId) {
            return Optional.ofNullable(failure);
        }

        @Override
        public Optional<Throwable> lost(String pipelineId) {
            return Optional.empty();
        }

        /** Always carrying: these cases are about what the driver does with a converge result. */
        @Override
        public boolean isCarryingAJob(String pipelineId) {
            return true;
        }
    }
}
