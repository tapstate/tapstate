package io.tapstate.app;

import ch.qos.logback.classic.Logger;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.control.core.PipelineExplanation.PendingReason;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.runtime.scheduler.StartDeferred;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
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
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() - deadline < 0) {
            driver.reconcile();
            if (state.read(pipelineId).map(checkpoint -> StateJson.parse(checkpoint.stateJson()))
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
    void aMissingPausedJobReachesTheStoreBackedStatusWithItsCodedReason() {
        AtomicBoolean carrying = new AtomicBoolean(true);
        LifecycleActuator job = new LifecycleActuator() {
            @Override public void start(String id) { carrying.set(true); }
            @Override public void pause(String id) { }
            @Override public void resume(String id) { }
            @Override public void stop(String id, boolean purgeState) { carrying.set(false); }
            @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
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

        /** Always carrying: these cases are about what the driver does with a converge result. */
        @Override
        public boolean isCarryingAJob(String pipelineId) {
            return true;
        }
    }
}
