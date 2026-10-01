package io.tapstate.app;

import io.tapstate.control.core.PipelineIncarnationService;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.StartDeferred;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.srs.SnapshotCapacityUnavailable;
import io.tapstate.runtime.srs.PhysicalRingNotReady;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * Binds the converge loop's lifecycle actuator seam to the Jet execution engine and the source-side capture
 * coordinator, composing the two so the data plane runs end to end. This is the assembly-layer wiring of the
 * data plane, which lets the runtime ring's converge loop stay engine- and framework-free.
 *
 * <p>Each verb composes the two sides in the order the data flow requires:
 * <ul>
 *   <li>{@code start} validates before side effects, finishes an earlier drop, provisions capture and
 *       reserves its snapshot data-plane work, submits the job, then activates the reader. The source holds
 *       CDC behind the snapshot completion marker.</li>
 *   <li>{@code stop} cancels the job first (engine) then stops the capture behind it (coordinator), so the
 *       capture daemon is torn down only once nothing reads its ring; the operator state the run kept is
 *       let go of last, once the job it belonged to is actually over -- and only where the stop asked for
 *       it. A stop that was asked to keep the state does not write the drop down either, which is a
 *       stronger thing than not carrying it out: the note is what a later start finishes, so one written
 *       here would have the state dropped by the next start of a pipeline nobody asked to clear.</li>
 *   <li>{@code pause} / {@code resume} are engine-only once the initial load has reached the target: the
 *       capture keeps running while a pipeline is paused, held back by the ring's headroom backpressure,
 *       and a resume replays the buffered ring. A load still undelivered is the one case that cannot be
 *       resumed in place, and it rebuilds instead.</li>
 * </ul>
 */
final class EngineLifecycleActuator implements LifecycleActuator {

    private static final Logger LOG = LoggerFactory.getLogger(EngineLifecycleActuator.class);

    /**
     * How long a stop waits for the job to actually be over before it gives up on letting go of that job's
     * state in the same breath. Long enough that an ordinary cancel finishes inside it, short enough that
     * a job which will not die does not hold the converge loop: what is not dropped here is left noted and
     * dropped by the next start instead.
     */
    private static final Duration JOB_TEARDOWN_BUDGET = Duration.ofSeconds(30);

    private final Engine engine;
    private final DagSource dagSource;
    private final PipelineCaptureCoordinator captureCoordinator;
    private final NestStateTeardown stateTeardown;
    private final PipelineActuationOwnership actuation;
    private final PipelineIncarnationService incarnations;
    private final ObservationScopeRegistry observationScopes;
    private final ObservationPublisher observationPublisher;
    private final ObservationStore observations;

    EngineLifecycleActuator(Engine engine, DagSource dagSource, PipelineCaptureCoordinator captureCoordinator,
            NestStateTeardown stateTeardown, PipelineActuationOwnership actuation) {
        this(engine, dagSource, captureCoordinator, stateTeardown, actuation, null, null);
    }

    EngineLifecycleActuator(Engine engine, DagSource dagSource, PipelineCaptureCoordinator captureCoordinator,
            NestStateTeardown stateTeardown, PipelineActuationOwnership actuation,
            PipelineIncarnationService incarnations, ObservationScopeRegistry observationScopes) {
        this(engine, dagSource, captureCoordinator, stateTeardown, actuation, incarnations,
                observationScopes, null, null);
    }

    EngineLifecycleActuator(Engine engine, DagSource dagSource, PipelineCaptureCoordinator captureCoordinator,
            NestStateTeardown stateTeardown, PipelineActuationOwnership actuation,
            PipelineIncarnationService incarnations, ObservationScopeRegistry observationScopes,
            ObservationPublisher observationPublisher, ObservationStore observations) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.dagSource = Objects.requireNonNull(dagSource, "dagSource");
        this.captureCoordinator = Objects.requireNonNull(captureCoordinator, "captureCoordinator");
        this.stateTeardown = Objects.requireNonNull(stateTeardown, "stateTeardown");
        this.actuation = Objects.requireNonNull(actuation, "actuation");
        this.incarnations = incarnations;
        this.observationScopes = observationScopes;
        this.observationPublisher = observationPublisher;
        this.observations = observations;
    }

    @Override
    public void start(String pipelineId) {
        try (PreparedStart prepared = prepareStart(pipelineId)) {
            prepared.submit();
        } catch (StartDeferred waiting) {
            // Direct callers retain the existing retry behavior. Convergence uses prepareStart itself
            // so a deferred start never advances the actual checkpoint to RUNNING.
        }
    }

    @Override
    public PreparedStart prepareStart(String pipelineId) {
        // Submitting by name is idempotent, but preparing another execution before this check would
        // move its fence while the existing Jet job and capture still use the previous one.
        if (engine.hasLiveJob(pipelineId)) {
            return new PreparedStart() {
                @Override public void submit() { }
                @Override public void close() { }
            };
        }
        // Validation precedes teardown, capture, and submission. An unmet source prerequisite leaves
        // no data-plane component running and no start-side state mutation behind.
        DagSource.StartPreparation prepared = dagSource.prepareStart(
                pipelineId, stateTeardown.defaultDatabase());
        String incarnation = incarnations == null ? null : incarnations.ensureCurrent(pipelineId)
                .orElseThrow(() -> new IllegalStateException("validated pipeline lost its artifact before start"));
        return startPrepared(pipelineId, prepared, incarnation);
    }

    private PreparedStart startPrepared(String pipelineId, DagSource.StartPreparation prepared,
            String incarnation) {
        if (captureCoordinator.hasActiveCapture(pipelineId)) {
            // A prior job can die while its source capture remains open. Close that run before opening
            // another so its reader cursor is not reused by a new job that resumes from an earlier sink ACK.
            captureCoordinator.stopCapture(pipelineId, false);
        }
        stateTeardown.finishPending(pipelineId);
        DagSource.NestCapacity capacity = prepared.capacity();
        engine.configureNestState(capacity.mapDatabases(), capacity.settings());
        if (!capacity.mapDatabases().isEmpty()) {
            LOG.info("Nest state placement resolved before pipeline '{}' starts: {}",
                    pipelineId, capacity.mapDatabases());
        }
        stateTeardown.willKeepStateAt(pipelineId, prepared.stateLocations());
        try {
            prepared.artifactSnapshot().ifPresentOrElse(
                    snapshot -> captureCoordinator.startCapture(
                            pipelineId, snapshot, prepared.cursorWriterToken()),
                    () -> captureCoordinator.startCapture(pipelineId));
        } catch (SnapshotCapacityUnavailable unavailable) {
            throw new StartDeferred(StartDeferred.Reason.CAPACITY);
        } catch (RingNotOpenYet | PhysicalRingNotReady notYet) {
            throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
        }
        if (Thread.currentThread().isInterrupted()) {
            closeCaptureAfterCancellation(pipelineId);
            throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
        }
        // Capacity and physical-ring admission are settled before advancing the durable execution
        // generation. A refused attempt is still a pending start, not a new data-plane execution.
        PipelineActuationOwnership.Execution execution;
        try {
            execution = actuation.beginExecution(pipelineId);
        } catch (RuntimeException | Error failure) {
            try {
                captureCoordinator.stopCapture(pipelineId, false);
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        if (!execution.allowed()) {
            captureCoordinator.stopCapture(pipelineId, false);
            LOG.warn("Not starting pipeline {} on this member: its run could not be fenced to a new "
                    + "execution generation", pipelineId);
            throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
        }
        ObservationStore.Scope observationScope;
        try {
            observationScope = observationScopes == null ? null
                    : observationScopes.begin(pipelineId, incarnation, execution.fence().executionGeneration());
        } catch (RuntimeException | Error failure) {
            try {
                captureCoordinator.stopCapture(pipelineId, false);
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        // Capture opens the SRS generation that source vertices compile into the DAG. Build from the
        // same frozen artifacts after provisioning, but submit only when the checkpoint CAS succeeds.
        PipelineLogContext previousLogContext = PipelineLogContext.capture();
        DagSource.StartPlan plan;
        try {
            PipelineLogContext.bindScope(observationScope);
            plan = prepared.build(execution.fence());
        } catch (RuntimeException | Error failure) {
            try {
                captureCoordinator.stopCapture(pipelineId, false);
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            if (observationScope != null) {
                observationScopes.discard(pipelineId, observationScope);
            }
            throw failure;
        } finally {
            previousLogContext.restore();
        }
        if (Thread.currentThread().isInterrupted()) {
            closeCaptureAfterCancellation(pipelineId);
            if (observationScope != null) {
                observationScopes.discard(pipelineId, observationScope);
            }
            throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
        }
        return new PreparedStart() {
            private boolean submitted;
            private boolean closed;

            @Override
            public void submit() {
                if (closed || submitted) {
                    throw new IllegalStateException("prepared pipeline start was closed or already submitted");
                }
                submitted = true;
                PipelineLogContext submitLogContext = PipelineLogContext.capture();
                try {
                    PipelineLogContext.bindScope(observationScope);
                    if (observationScope == null) {
                        engine.submit(pipelineId, plan.dag(), capacity.mapDatabases(), capacity.settings());
                    } else {
                        engine.submit(pipelineId, plan.dag(), capacity.mapDatabases(), capacity.settings(),
                                actuation.clusterId(), observationScope);
                    }
                    captureCoordinator.activateSnapshot(pipelineId);
                } catch (RuntimeException | Error failure) {
                    // A submitted job or reserved snapshot may already exist. Give both back before
                    // another convergence pass retries.
                    try {
                        engine.cancel(pipelineId);
                    } catch (RuntimeException cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                    try {
                        captureCoordinator.stopCapture(pipelineId, false);
                    } catch (RuntimeException cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                    if (observationScope != null) {
                        observationScopes.discard(pipelineId, observationScope);
                    }
                    throw failure;
                } finally {
                    submitLogContext.restore();
                }
            }

            @Override
            public void close() {
                if (closed) {
                    return;
                }
                closed = true;
                if (!submitted) {
                    PipelineLogContext closeLogContext = PipelineLogContext.capture();
                    try {
                        PipelineLogContext.bindScope(observationScope);
                        captureCoordinator.stopCapture(pipelineId, false);
                    } finally {
                        try {
                            if (observationScope != null) {
                                observationScopes.discard(pipelineId, observationScope);
                            }
                        } finally {
                            closeLogContext.restore();
                        }
                    }
                }
            }
        };
    }

    private void closeCaptureAfterCancellation(String pipelineId) {
        // The capture teardown may wait; let it run without the worker's cancellation flag, then
        // preserve that flag for the dispatcher. The interrupted start never owns a state purge.
        boolean interrupted = Thread.interrupted();
        try {
            captureCoordinator.stopCapture(pipelineId, false);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void pause(String pipelineId) {
        captureCurrentMetrics(pipelineId);
        engine.suspend(pipelineId);
    }

    /**
     * Resumes the pipeline, rebuilding rather than carrying on when its initial load has not reached the
     * target yet.
     *
     * <p>The rows a load has read but not delivered live nowhere durable. They reach the source vertex
     * through a member-local hand-off that vertex consumes once, and resuming restarts the job under a
     * guarantee that keeps no execution state -- so the vertex that comes back finds an empty hand-off
     * over a capture that has moved on to tailing. It then reads nothing at all, indefinitely, with the
     * job running and nothing thrown.
     *
     * <p>Rebuilding is what a stop that keeps and a start already do correctly here: the tables the record
     * still owes are read again and nothing is cleared. Re-reading them is the right cost rather than a
     * regression -- every row of a snapshot carries one reserved position, so nothing anywhere represents
     * a table loaded part way, and avoiding the re-read would mean inventing a durable per-row notion of
     * load progress plus a new ordering question for every consumer of the chain.
     */
    @Override
    public void resume(String pipelineId) {
        if (!captureCoordinator.loadDelivered(pipelineId)) {
            stopForRebuildingResume(pipelineId, false);
            start(pipelineId);
            return;
        }
        engine.resume(pipelineId);
    }

    @Override
    public boolean needsRebuildOnResume(String pipelineId) {
        return !captureCoordinator.loadDelivered(pipelineId);
    }

    @Override
    public void stop(String pipelineId, boolean purgeState) {
        captureCurrentMetrics(pipelineId);
        if (observationScopes != null) {
            observationScopes.clearContinuation(pipelineId);
        }
        if (observationPublisher != null) {
            observationPublisher.clearRebuildingResume(pipelineId);
        }
        stopInternal(pipelineId, purgeState);
    }

    @Override
    public void stopForRebuildingResume(String pipelineId, boolean purgeState) {
        captureMetricsForResume(pipelineId);
        stopInternal(pipelineId, purgeState);
    }

    @Override
    public Optional<StopAuthority> stopAuthority(String pipelineId) {
        return actuation.stopAuthority(pipelineId);
    }

    @Override
    public Optional<StopReservation.Subject> stopSubject(String pipelineId) {
        Optional<Engine.ExecutionJob> nativeJob = engine.executionJob(pipelineId);
        StopAuthority authority = stopAuthority(pipelineId).orElse(null);
        if (nativeJob.isEmpty()) {
            return Optional.of(new StopReservation.NoJob(actuation.clusterId(), authority));
        }
        Engine.ExecutionJob old = nativeJob.orElseThrow();
        boolean sameExecution = authority != null && authority.clusterId().equals(old.job().clusterId())
                && authority.executionGeneration() == old.scope().executionGeneration()
                && (incarnations == null || incarnations.current(pipelineId)
                        .filter(old.scope().pipelineIncarnationId()::equals).isPresent());
        if (sameExecution) {
            // A qualified terminal job still supplies the cumulative floor for a rebuilding resume.
            return Optional.of(new StopReservation.ExistingJob(old.scope().pipelineIncarnationId(),
                    old.scope().executionGeneration(), old.job(), authority));
        }
        if (engine.noUnfinishedJob(pipelineId)) {
            return Optional.of(new StopReservation.NoJob(actuation.clusterId(), authority));
        }
        throw new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", pipelineId), null);
    }

    @Override
    public boolean finishStop(StopReservation reservation, boolean continuing, boolean firstAttempt,
            boolean retiring, BooleanSupplier current) {
        String id = reservation.pipelineId();
        if (!current.getAsBoolean()) { return false; }
        Engine.ExecutionJob old = reservation.subject() instanceof StopReservation.ExistingJob existing
                ? new Engine.ExecutionJob(existing.oldJob(), new ObservationStore.Scope(
                        existing.pipelineIncarnationId(), existing.executionGeneration())) : null;
        if (old == null) {
            if (!engine.noUnfinishedJob(id)) { return false; }
        } else {
            if (continuing && !prepareStopContinuation(id, old, firstAttempt, current)) {
                return false;
            }
            if (firstAttempt && !continuing && engine.isCurrentOrAbsent(id, old)) {
                captureCurrentMetrics(id);
            }
            if (!current.getAsBoolean()) { return false; }
            engine.cancelExact(id, old);
            if (!engine.awaitTerminalExact(id, old, JOB_TEARDOWN_BUDGET)) { return false; }
            if (!engine.isCurrentOrAbsent(id, old)) {
                // A newer native job owns pipeline-local capture. Retiring old work may only close its old job.
                return retiring && current.getAsBoolean();
            }
        }
        if (!current.getAsBoolean()) { return false; }
        if (old == null && !engine.noUnfinishedJob(id) || old != null && !engine.isCurrentOrAbsent(id, old)) {
            return false;
        }
        boolean purge = !retiring && reservation.originalDesired().purgeState();
        if (!continuing) {
            if (observationScopes != null) { observationScopes.clearContinuation(id); }
            if (observationPublisher != null) { observationPublisher.clearRebuildingResume(id); }
        }
        captureCoordinator.stopCapture(id, purge);
        if (captureCoordinator.hasActiveCapture(id) || !current.getAsBoolean()) { return false; }
        if (purge) {
            stateTeardown.noteLocations(id, dagSource.stateLocations(id, stateTeardown.defaultDatabase()));
            if (!current.getAsBoolean()) { return false; }
            stateTeardown.finishPending(id);
        }
        return current.getAsBoolean();
    }

    private boolean prepareStopContinuation(String id, Engine.ExecutionJob old, boolean firstAttempt,
            BooleanSupplier current) {
        if (observationScopes == null) { return current.getAsBoolean(); }
        Optional<ObservationStore.Scope> scope = observationScopes.current(id);
        if (scope.isPresent()) {
            if (!scope.orElseThrow().equals(old.scope()) || !current.getAsBoolean()) { return false; }
            if (firstAttempt) { captureMetricsForResume(id); }
            return current.getAsBoolean();
        }
        BooleanSupplier qualified = () -> current.getAsBoolean() && engine.isCurrentOrAbsent(id, old)
                && (incarnations == null || incarnations.current(id)
                        .filter(old.scope().pipelineIncarnationId()::equals).isPresent());
        var ticket = observationScopes.beginColdRebuild(id, old.scope(), qualified).orElse(null);
        if (ticket == null) { return false; }
        try {
            Optional<ObservationStore.Stored> stored = Optional.empty();
            if (observations != null) {
                try {
                    stored = observations.readStored(id);
                } catch (RuntimeException unavailable) {
                    LOG.warn("Could not read the known cumulative metrics for pipeline {}", id, unavailable);
                }
            }
            if (!observationScopes.prepareColdRebuildingResume(ticket, stored) || !qualified.getAsBoolean()) {
                observationScopes.cancelColdRebuild(ticket);
                return false;
            }
            if (observationPublisher != null) { observationPublisher.prepareRebuildingResume(id); }
            return true;
        } catch (RuntimeException | Error failure) {
            observationScopes.cancelColdRebuild(ticket);
            throw failure;
        }
    }

    private void captureMetricsForResume(String pipelineId) {
        if (observationScopes == null) {
            return;
        }
        Optional<ObservationStore.Scope> owner = observationScopes.current(pipelineId);
        if (owner.isEmpty()) {
            return;
        }
        captureCurrentMetrics(pipelineId, owner.get());
        Optional<ObservationStore.Stored> stored = Optional.empty();
        if (observations != null && observationScopes.needsStoredFallback(pipelineId)) {
            try {
                stored = observations.readStored(pipelineId);
            } catch (RuntimeException unavailable) {
                LOG.warn("Could not read the last cumulative metrics for pipeline {}", pipelineId,
                        unavailable);
            }
        }
        observationScopes.prepareRebuildingResume(pipelineId, stored);
        if (observationPublisher != null) {
            observationPublisher.prepareRebuildingResume(pipelineId);
        }
    }

    /** Takes only local measurements before suspension or teardown releases their native producer. */
    private void captureCurrentMetrics(String pipelineId) {
        if (observationScopes != null) {
            observationScopes.current(pipelineId).ifPresent(owner -> captureCurrentMetrics(pipelineId, owner));
        }
    }

    private void captureCurrentMetrics(String pipelineId, ObservationStore.Scope owner) {
        if (observationPublisher != null) {
            try {
                observationPublisher.prepareScoped(pipelineId, null, owner,
                        () -> observationScopes.current(pipelineId).filter(owner::equals).isPresent())
                        .ifPresent(frame -> observationScopes.continueFrame(frame, owner));
            } catch (RuntimeException unavailable) {
                LOG.warn("Could not measure the final cumulative metrics for pipeline {}", pipelineId,
                        unavailable);
            }
        }
    }

    private void stopInternal(String pipelineId, boolean purgeState) {
        engine.cancel(pipelineId);
        boolean jobOver = engine.awaitTerminal(pipelineId, JOB_TEARDOWN_BUDGET);
        if (!jobOver) {
            throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
        }
        if (purgeState) {
            // Noted before the job is even known to be over, and before the drop: a stop is driven once, on
            // the transition, so a process that dies anywhere after this point leaves a note the next start
            // finishes. What the runs said they keep is the half that survives an edit; what the pipeline
            // compiles to now is the half that covers state older than there being anywhere to say it. The
            // note takes both.
            stateTeardown.noteLocations(
                    pipelineId, dagSource.stateLocations(pipelineId, stateTeardown.defaultDatabase()));
        }
        captureCoordinator.stopCapture(pipelineId, purgeState);
        if (purgeState) {
            // Only once nothing is left to write into it. A processor still winding down writes state as it
            // closes, and a drop racing that leaves entries behind with the note already gone.
            stateTeardown.finishPending(pipelineId);
        }
    }

    @Override
    public Optional<Throwable> failure(String pipelineId) {
        // A pipeline fails two ways the converge loop must see as one: the Jet job itself dies (engine), or the
        // cdc capture feeding its ring dies while the job keeps running over a ring gone quiet (coordinator).
        // Either surfaces here so the converge side drives the pipeline into the observable FAILED state.
        return engine.failureOf(pipelineId).or(() -> captureCoordinator.captureFailure(pipelineId));
    }

    @Override
    public boolean isCarryingAJob(String pipelineId) {
        // The job side alone. A capture that died while the job runs is a failure, reported above; what
        // is asked here is whether anything is running this pipeline at all, and a process that has just
        // come up to a checkpoint an earlier one wrote answers no.
        return engine.hasLiveJob(pipelineId);
    }
}
