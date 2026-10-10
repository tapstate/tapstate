package io.tapstate.app;

import io.tapstate.control.core.PipelineIncarnationService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.model.BatchSpec;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.StartDeferred;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.SuccessorAdmission;
import io.tapstate.spi.store.HandoffIdentity;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.srs.SnapshotCapacityUnavailable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.Set;

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
    private final ExecutionPlanRecorder plans;
    private final Clock clock;
    private final ConnectorReadiness connectors;

    EngineLifecycleActuator(Engine engine, DagSource dagSource, PipelineCaptureCoordinator captureCoordinator,
            NestStateTeardown stateTeardown) {
        this(engine, dagSource, captureCoordinator, stateTeardown, PipelineActuationOwnership.single());
    }

    EngineLifecycleActuator(Engine engine, DagSource dagSource, PipelineCaptureCoordinator captureCoordinator,
            NestStateTeardown stateTeardown, PipelineActuationOwnership actuation, ExecutionPlanRecorder plans,
            Clock clock) {
        this(engine, dagSource, captureCoordinator, stateTeardown, actuation, plans, clock, ConnectorReadiness.NONE);
    }

    EngineLifecycleActuator(Engine engine, DagSource dagSource, PipelineCaptureCoordinator captureCoordinator,
            NestStateTeardown stateTeardown, PipelineActuationOwnership actuation, ExecutionPlanRecorder plans,
            Clock clock, ConnectorReadiness connectors) {
        this(engine, dagSource, captureCoordinator, stateTeardown, actuation, null, null, null, null, null,
                plans, clock, connectors);
    }

    private final PipelineIncarnationService incarnations;
    private final ObservationScopeRegistry observationScopes;
    private final ObservationPublisher observationPublisher;
    private final ObservationStore observations;
    private final io.tapstate.spi.store.ExecutionGenerationStore generations;

    EngineLifecycleActuator(Engine engine, DagSource dagSource, PipelineCaptureCoordinator captureCoordinator,
            NestStateTeardown stateTeardown, PipelineActuationOwnership actuation) {
        this(engine, dagSource, captureCoordinator, stateTeardown, actuation, null, null, null, null, null);
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
        this(engine, dagSource, captureCoordinator, stateTeardown, actuation, incarnations, observationScopes,
                observationPublisher, observations, null);
    }

    EngineLifecycleActuator(Engine engine, DagSource dagSource, PipelineCaptureCoordinator captureCoordinator,
            NestStateTeardown stateTeardown, PipelineActuationOwnership actuation,
            PipelineIncarnationService incarnations, ObservationScopeRegistry observationScopes,
            ObservationPublisher observationPublisher, ObservationStore observations,
            io.tapstate.spi.store.ExecutionGenerationStore generations) {
        this(engine, dagSource, captureCoordinator, stateTeardown, actuation, incarnations, observationScopes,
                observationPublisher, observations, generations, ExecutionPlanRecorder.NONE,
                Clock.systemUTC(), ConnectorReadiness.NONE);
    }

    EngineLifecycleActuator(Engine engine, DagSource dagSource, PipelineCaptureCoordinator captureCoordinator,
            NestStateTeardown stateTeardown, PipelineActuationOwnership actuation,
            PipelineIncarnationService incarnations, ObservationScopeRegistry observationScopes,
            ObservationPublisher observationPublisher, ObservationStore observations,
            io.tapstate.spi.store.ExecutionGenerationStore generations, ExecutionPlanRecorder plans,
            Clock clock, ConnectorReadiness connectors) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.dagSource = Objects.requireNonNull(dagSource, "dagSource");
        this.captureCoordinator = Objects.requireNonNull(captureCoordinator, "captureCoordinator");
        this.stateTeardown = Objects.requireNonNull(stateTeardown, "stateTeardown");
        this.actuation = Objects.requireNonNull(actuation, "actuation");
        this.incarnations = incarnations;
        this.observationScopes = observationScopes;
        this.observationPublisher = observationPublisher;
        this.observations = observations;
        this.generations = generations;
        this.plans = Objects.requireNonNull(plans, "plans");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.connectors = Objects.requireNonNull(connectors, "connectors");
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
        return prepareStart(pipelineId, null, null, ignored -> { });
    }

    @Override
    public PreparedStart prepareStart(String pipelineId, io.tapstate.core.lifecycle.DesiredState desired,
            io.tapstate.core.lifecycle.CheckpointDoc checkpoint,
            java.util.function.Consumer<io.tapstate.spi.store.PreExecutionFailure.Attempt> captured) {
        engine.refuseIfLost(pipelineId);
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
        String incarnation = incarnations == null ? null : incarnations.ensureCurrent(pipelineId)
                .orElseThrow(() -> new IllegalStateException("validated pipeline lost its artifact before start"));
        DagSource.StartPreparation prepared;
        try {
            prepared = prepareInputs(pipelineId, incarnation, desired, checkpoint, captured);
        } catch (TapstateException refused) {
            actuation.startRefusedBeforeItsRun(pipelineId);
            throw refused;
        }
        return startPrepared(pipelineId, prepared, incarnation, null, null, null, captured);
    }

    private PreparedStart startPrepared(String pipelineId, DagSource.StartPreparation prepared,
            String incarnation) {
        return startPrepared(pipelineId, prepared, incarnation, null, null, null);
    }

    @Override
    public PreparedReplacement prepareReplacement(StopReservation reservation, ReplacementAdmission admission,
            Predicate<StopReservation> current) {
        return prepareReplacement(reservation, admission, current, null, ignored -> { });
    }

    @Override
    public PreparedReplacement prepareReplacement(StopReservation reservation, ReplacementAdmission admission,
            Predicate<StopReservation> current, io.tapstate.core.lifecycle.CheckpointDoc checkpoint,
            java.util.function.Consumer<io.tapstate.spi.store.PreExecutionFailure.Attempt> captured) {
        Objects.requireNonNull(reservation, "reservation");
        Objects.requireNonNull(admission, "admission");
        Objects.requireNonNull(current, "current");
        String pipelineId = reservation.pipelineId();
        engine.refuseIfLost(pipelineId);
        if (reservation.phase() != StopReservation.Phase.REPLACEMENT_PENDING || engine.hasLiveJob(pipelineId)) {
            throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
        }
        String incarnation = incarnations == null ? null : incarnations.current(pipelineId).orElse(null);
        if (incarnation == null) {
            throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
        }
        DagSource.StartPreparation prepared = prepareInputs(pipelineId, incarnation, reservation.originalDesired(),
                checkpoint, captured);
        return startPrepared(pipelineId, prepared, incarnation, reservation, admission, current, captured);
    }

    private DagSource.StartPreparation prepareInputs(String id, String incarnation,
            io.tapstate.core.lifecycle.DesiredState desired, io.tapstate.core.lifecycle.CheckpointDoc checkpoint,
            java.util.function.Consumer<io.tapstate.spi.store.PreExecutionFailure.Attempt> captured) {
        if (generations == null || incarnation == null || desired == null || checkpoint == null) {
            return dagSource.prepareStart(id, stateTeardown.defaultDatabase());
        }
        PipelineActuationOwnership.Permit permit = actuation.permit(id);
        if (!permit.granted()) { throw new StartDeferred(StartDeferred.Reason.DEPENDENCY); }
        var frontier = generations.currentGeneration(actuation.clusterId(), id);
        var writer = permit.claim() == null ? null : io.tapstate.spi.store.WorkloadClaimFence.from(permit.claim());
        if (writer != null && writer.executionGeneration() != frontier.orElse(0L)) {
            throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
        }
        return dagSource.prepareStart(id, stateTeardown.defaultDatabase(), snapshot -> {
            if (snapshot.get(id).filter(resource -> "pipeline".equals(resource.kind())).isEmpty()
                    || incarnations.current(id).filter(incarnation::equals).isEmpty()) {
                throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
            }
            captured.accept(new io.tapstate.spi.store.PreExecutionFailure.Attempt(id, actuation.clusterId(), incarnation,
                        checkpoint, desired, io.tapstate.control.core.ObservationArtifactLineage.hashes(id, snapshot.list()),
                        frontier, writer));
        });
    }

    @Override
    public boolean stillPreExecution(io.tapstate.spi.store.PreExecutionFailure.Attempt attempt) {
        return generations != null && !engine.hasLiveJob(attempt.pipelineId())
                && incarnations.current(attempt.pipelineId()).filter(attempt.pipelineIncarnationId()::equals).isPresent()
                && attempt.generationFrontier().equals(generations.currentGeneration(attempt.clusterId(), attempt.pipelineId()));
    }

    private PreparedReplacement startPrepared(String pipelineId, DagSource.StartPreparation prepared,
            String incarnation, StopReservation replacement, ReplacementAdmission admission,
            Predicate<StopReservation> currentAdmission) {
        return startPrepared(pipelineId, prepared, incarnation, replacement, admission, currentAdmission, ignored -> { });
    }

    private PreparedReplacement startPrepared(String pipelineId, DagSource.StartPreparation prepared,
            String incarnation, StopReservation replacement, ReplacementAdmission admission,
            Predicate<StopReservation> currentAdmission,
            java.util.function.Consumer<io.tapstate.spi.store.PreExecutionFailure.Attempt> captured) {
        Set<String> sharedConnectors;
        try {
            sharedConnectors = connectors.requireEveryMemberCanLoad(
                    pipelineId, Set.copyOf(prepared.sinkConnectors().values()));
        } catch (TapstateException refused) {
            actuation.startRefusedBeforeItsRun(pipelineId);
            throw refused;
        }
        if (captureCoordinator.hasActiveCapture(pipelineId) || captureCoordinator.isCapturing(pipelineId)) {
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
        // The run is worked out before its capture opens - how wide each node runs among the members, and every
        // shape it records - so a start refused here, for a width it cannot honour or anything else found on the
        // way, has opened no source connector and left the pipeline on no mining chain. A consumer left there
        // holds back every other pipeline reading the same table on that chain. Worked out after placement and
        // teardown, from the same frozen artifacts, so any shape record it writes stays named if it refuses.
        DagSource.PlannedStart planned;
        try {
            planned = prepared.plan();
        } catch (TapstateException refused) {
            actuation.startRefusedBeforeItsRun(pipelineId);
            throw refused;
        }
        try {
            prepared.artifactSnapshot().ifPresentOrElse(
                    snapshot -> captureCoordinator.startCapture(
                            pipelineId, snapshot, prepared.cursorWriterToken()),
                    () -> captureCoordinator.startCapture(pipelineId));
        } catch (SnapshotCapacityUnavailable unavailable) {
            throw new StartDeferred(StartDeferred.Reason.CAPACITY);
        } catch (RingNotOpenYet notYet) {
            throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
        }
        if (Thread.currentThread().isInterrupted()) {
            closeCaptureAfterCancellation(pipelineId);
            throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
        }
        // Capacity and physical-ring admission are settled before advancing the durable execution
        // generation. A refused attempt is still a pending start, not a new data-plane execution.
        PipelineActuationOwnership.Execution execution;
        SuccessorAdmission admitted;
        try {
            if (replacement == null) {
                execution = actuation.beginExecution(pipelineId);
                admitted = null;
            } else {
                StopAuthority current = actuation.stopAuthority(pipelineId).orElse(null);
                java.util.Set<String> members = actuation.plannedExecutionMembers(pipelineId, current)
                        .orElseThrow(() -> new StartDeferred(StartDeferred.Reason.DEPENDENCY));
                admitted = admission.admit(current, incarnation, engine.submissionBootId(), members)
                        .orElseThrow(() -> new StartDeferred(StartDeferred.Reason.DEPENDENCY));
                execution = actuation.adoptAdmission(admitted);
            }
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
        captured.accept(null);
        ObservationStore.Scope observationScope;
        try {
            if (observationScopes == null) {
                observationScope = null;
            } else if (replacement != null && replacement.counterPolicy() == StopReservation.CounterPolicy.RESET) {
                observationScope = observationScopes.beginResetExecution(pipelineId,
                        new ObservationStore.Scope(incarnation, execution.fence().executionGeneration()));
            } else {
                observationScope = observationScopes.begin(pipelineId, incarnation, execution.fence().executionGeneration());
            }
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
            plan = planned.build(execution.fence());
        } catch (RuntimeException | Error failure) {
            try {
                captureCoordinator.stopCapture(pipelineId, false);
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            if (observationScope != null) {
                if (replacement != null || !(failure instanceof TapstateException)) {
                    observationScopes.discard(pipelineId, observationScope);
                } else if (execution.fence().claimGeneration() == 0 || execution.admittedClaim().isPresent()) {
                    observationScopes.rememberFailedAdmission(pipelineId, observationScope,
                            execution.admittedClaim().map(ObservationScopeRecovery.Owner::of).orElse(null));
                }
            }
            // A coded build refusal after ordinary admission belongs to the real allocated scope.
            // Its FAILED checkpoint and current authority are qualified on the telemetry worker.
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
        SuccessorAdmission accepted = admitted;
        return new PreparedReplacement() {
            private boolean submitted;
            private boolean closed;
            private Engine.ExecutionJob submittedExecution;

            @Override public StopReservation admitted() {
                if (accepted == null) { throw new IllegalStateException("ordinary start has no replacement admission"); }
                return accepted.reservation();
            }

            @Override public Optional<StopReservation.JobIdentity> submittedJob() {
                return submitted ? engine.executionJob(pipelineId).filter(job -> observationScope != null
                        && observationScope.equals(job.scope())).map(Engine.ExecutionJob::job) : Optional.empty();
            }

            @Override public Optional<StopReservation.Source> submittedSource() {
                return Optional.ofNullable(submittedExecution).map(job -> new StopReservation.Source(
                        job.job().clusterId(), job.scope(), job.job()));
            }

            @Override
            public void submit() {
                if (closed || submitted) {
                    throw new IllegalStateException("prepared pipeline start was closed or already submitted");
                }
                if (accepted != null && (!currentAdmission.test(accepted.reservation())
                        || !Objects.equals(accepted.reservation().writerAuthority(),
                                actuation.stopAuthority(pipelineId).orElse(null)))) {
                    throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
                }
                submitted = true;
                PipelineLogContext submitLogContext = PipelineLogContext.capture();
                try {
                    PipelineLogContext.bindScope(observationScope);
                    if (!actuation.proveExecution(execution.fence())) {
                        throw new StartDeferred(StartDeferred.Reason.DEPENDENCY);
                    }
                    plans.record(planOf(pipelineId, execution, plan.planned(), clock.instant(),
                            prepared.sinkConnectors(), sharedConnectors).replacing(plans.last(pipelineId)));
                    if (observationScope == null) {
                        engine.submit(pipelineId, plan.dag(), capacity.mapDatabases(), capacity.settings());
                    } else {
                        if (execution.executionNodeIds().isEmpty()) {
                            engine.submit(pipelineId, plan.dag(), capacity.mapDatabases(), capacity.settings(),
                                    actuation.clusterId(), observationScope);
                        } else {
                            engine.submit(pipelineId, plan.dag(), capacity.mapDatabases(), capacity.settings(),
                                    actuation.clusterId(), observationScope, execution.executionNodeIds());
                        }
                        submittedExecution = engine.executionJob(pipelineId).filter(job -> observationScope.equals(job.scope())
                                && actuation.clusterId().equals(job.job().clusterId())
                                && engine.submissionBootId().equals(job.job().bootId())).orElseThrow(
                                        () -> new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,
                                                Map.of("pipeline", pipelineId), null));
                    }
                    if (observationScope == null) {
                        captureCoordinator.activateSnapshot(pipelineId);
                    } else {
                        captureCoordinator.activateSnapshot(pipelineId, new io.tapstate.core.logging.LogSink.Scope(
                                observationScope.pipelineIncarnationId(), observationScope.executionGeneration()));
                    }
                } catch (RuntimeException | Error failure) {
                    // A submitted job or reserved snapshot may already exist. Give both back before
                    // another convergence pass retries.
                    try {
                        if (submittedExecution != null) {
                            engine.cancelExact(pipelineId, submittedExecution);
                        } else if (observationScope == null) {
                            engine.cancel(pipelineId);
                        }
                    } catch (RuntimeException cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                    try {
                        if ((accepted == null || currentAdmission.test(accepted.reservation()))
                                && (observationScope == null
                                        || submittedExecution == null && engine.noUnfinishedJob(pipelineId)
                                        || submittedExecution != null && engine.isCurrentOrAbsent(pipelineId, submittedExecution))) {
                            captureCoordinator.stopCapture(pipelineId, false);
                        }
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
    public Optional<StopReservation.Source> stopSource(String pipelineId) {
        StopReservation.Subject subject = stopSubject(pipelineId).orElseThrow();
        return Optional.of(switch (subject) {
            case StopReservation.ExistingJob old -> new StopReservation.Source(old.oldJob().clusterId(),
                    new ObservationStore.Scope(old.pipelineIncarnationId(), old.executionGeneration()), old.oldJob());
            case StopReservation.NoJob absent -> {
                ObservationStore.Scope sourceScope = null;
                StopAuthority known = absent.knownAuthority();
                if (known != null && known.executionGeneration() > 0 && incarnations != null) {
                    sourceScope = incarnations.current(pipelineId)
                            .map(incarnation -> new ObservationStore.Scope(incarnation, known.executionGeneration()))
                            .orElse(null);
                }
                yield new StopReservation.Source(absent.clusterId(), sourceScope, null);
            }
        });
    }

    @Override
    public Optional<SuccessorInspection> inspectSuccessor(StopReservation reservation, BooleanSupplier current) {
        if (reservation.successor() == null || !current.getAsBoolean()) { return Optional.empty(); }
        String id = reservation.pipelineId();
        var slot = reservation.successor();
        var nativeJob = engine.executionJob(id);
        if (!current.getAsBoolean()) { return Optional.empty(); }
        if (nativeJob.isPresent()) {
            Engine.ExecutionJob observed = nativeJob.orElseThrow();
            boolean matches = slot.scope().equals(observed.scope())
                    && slot.submissionBootId().equals(observed.job().bootId())
                    && reservation.source().clusterId().equals(observed.job().clusterId())
                    && (slot.job() == null || slot.job().equals(observed.job()));
            if (matches) {
                var terminal = engine.terminalStateExact(id, observed);
                return current.getAsBoolean()
                        ? Optional.of(new SuccessorInspection(Optional.of(observed.job()), terminal)) : Optional.empty();
            }
            // Only terminal history can coexist with an admitted slot that was never submitted.
            if (!engine.noUnfinishedJob(id)) { return Optional.empty(); }
        }
        if (slot.job() != null && !engine.awaitTerminalExact(id,
                new Engine.ExecutionJob(slot.job(), slot.scope()), Duration.ZERO)) { return Optional.empty(); }
        return current.getAsBoolean() ? Optional.of(new SuccessorInspection(Optional.empty(), Optional.empty())) : Optional.empty();
    }

    @Override
    public boolean adoptSuccessor(StopReservation reservation, BooleanSupplier current) {
        var actual = inspectSuccessor(reservation, current).orElse(null);
        if (reservation.phase() != StopReservation.Phase.SUCCESSOR_BOUND || actual == null
                || actual.job().filter(reservation.successor().job()::equals).isEmpty()
                || !current.getAsBoolean()) { return false; }
        if (observationScopes == null) { return current.getAsBoolean(); }
        if (reservation.counterPolicy() == StopReservation.CounterPolicy.RESET) {
            if (observationScopes.current(reservation.pipelineId()).filter(reservation.successor().scope()::equals).isEmpty()
                    || observationScopes.activeContinuationKey(reservation.pipelineId()).isPresent()) {
                observationScopes.beginResetExecution(reservation.pipelineId(), reservation.successor().scope());
            }
        } else {
            var ticket = observationScopes.beginTargetContinuation(reservation.pipelineId(), continuationKey(reservation),
                    new ObservationScopeRegistry.ActualTarget(reservation.successor().scope(), reservation.successor().job()),
                    current).orElse(null);
            if (ticket == null) { return false; }
            var adopted = observationScopes.adoptTargetContinuation(ticket, Optional.empty(), Optional.empty());
            if (adopted.status() == ObservationScopeRegistry.PreparationStatus.INVALIDATED) { return false; }
        }
        return current.getAsBoolean();
    }

    @Override
    public Optional<HandoffIdentity> continuationReady(StopReservation reservation, BooleanSupplier current) {
        if (observationScopes == null || !current.getAsBoolean()) { return Optional.empty(); }
        return observationScopes.durableReceipt(reservation.pipelineId(), reservation.handoffIdentity())
                .filter(receipt -> current.getAsBoolean()).map(receipt -> reservation.handoffIdentity());
    }

    @Override
    public void observeReplacementFailure(StopReservation reservation, BooleanSupplier current) {
        var scope = reservation.successor() == null ? reservation.source().scope() : reservation.successor().scope();
        if (scope != null && observationScopes != null && current.getAsBoolean()
                && (incarnations == null || incarnations.current(reservation.pipelineId())
                        .filter(scope.pipelineIncarnationId()::equals).isPresent())) {
            observationScopes.begin(reservation.pipelineId(), scope.pipelineIncarnationId(), scope.executionGeneration());
        }
    }

    private static ObservationScopeRegistry.ContinuationKey continuationKey(StopReservation marker) {
        return new ObservationScopeRegistry.ContinuationKey(marker.token(), marker.source().scope(),
                marker.counterPolicy(), marker.writerAuthority());
    }

    @Override
    public boolean finishStop(StopReservation reservation, boolean continuing, boolean firstAttempt,
            boolean retiring, BooleanSupplier current) {
        String id = reservation.pipelineId();
        if (!current.getAsBoolean()) { return false; }
        Engine.ExecutionJob old;
        if (reservation.successor() != null) {
            var inspected = inspectSuccessor(reservation, current).orElse(null);
            if (inspected == null) { return false; }
            old = inspected.job().map(job -> new Engine.ExecutionJob(job, reservation.successor().scope())).orElse(null);
        } else {
            old = reservation.source().oldJob() == null ? null
                    : new Engine.ExecutionJob(reservation.source().oldJob(), reservation.source().scope());
        }
        if (continuing && !reservation.legacy() && observationScopes != null) {
            var previousSource = reservation.phase() == StopReservation.Phase.SUCCESSOR_BOUND
                    ? Optional.of(new ObservationScopeRegistry.ActualTarget(
                            reservation.successor().scope(), reservation.successor().job()))
                    : Optional.<ObservationScopeRegistry.ActualTarget>empty();
            var ticket = observationScopes.beginSourceContinuation(id, continuationKey(reservation),
                    previousSource, current).orElse(null);
            if (ticket != null) {
                // Already known local data is cheap. Durable reads and native collection belong to telemetry workers.
                observationScopes.prepareSourceContinuation(ticket, Optional.empty(), Optional.empty(),
                        ObservationScopeRegistry.SourceReadStatus.UNAVAILABLE);
            }
        }
        if (old == null) {
            if (!engine.noUnfinishedJob(id)) { return false; }
        } else {
            if (continuing && reservation.legacy() && !prepareStopContinuation(id, old, firstAttempt, current)) {
                return false;
            }
            if (reservation.legacy() && firstAttempt && !continuing && engine.isCurrentOrAbsent(id, old)) {
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
        if (!engine.isLost()) {
            ObservationStore.Scope ended = reservation.successor() == null
                    ? reservation.source().scope() : reservation.successor().scope();
            ExecutionPlan retained = plans.last(id);
            if (ended != null && retained != null
                    && Objects.equals(retained.executionGeneration(), ended.executionGeneration())
                    && current.getAsBoolean()) {
                plans.forget(id, retained);
            }
        }
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
        if (!engine.isLost()) {
            // A plan is kept on the engine's member, and a member shut down for want of memory refuses to be asked
            // about it - uncoded, and on every pass, which would keep the failure that took it down from ever being
            // recorded. What it held went with it; the start that follows the restart records the plan it runs.
            plans.forget(pipelineId);
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
        if (purgeState && jobOver && !engine.isLost()) {
            // Only once nothing is left to write into it. A processor still winding down writes state as it
            // closes, and a drop racing that leaves entries behind with the note already gone.
            //
            // Nor on an engine whose member was shut down for want of memory. Half of the drop is on that
            // member, which refuses it with an uncoded error, and "no job" there only means the member no
            // longer answers: its shutdown can still be waiting for the job to end. Left noted, the drop is
            // finished by the next start, which on a lost engine comes after the restart that is the only
            // way back.
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
    public Optional<Throwable> lost(String pipelineId) {
        // The engine alone: a member shut down for want of memory took every job it held with it. The capture is
        // not asked. It keeps running behind a paused pipeline, and whether it has died is failure()'s to say.
        return engine.lost(pipelineId).map(Throwable.class::cast);
    }

    @Override
    public boolean isCarryingAJob(String pipelineId) {
        // The job side alone. A capture that died while the job runs is a failure, reported above; what
        // is asked here is whether anything is running this pipeline at all, and a process that has just
        // come up to a checkpoint an earlier one wrote answers no.
        return engine.hasLiveJob(pipelineId);
    }

    /** As below, for a run none of whose sinks opens a connector named here. */
    static ExecutionPlan planOf(String pipelineId, PipelineActuationOwnership.Execution execution,
            DagSource.PlannedDag planned, Instant plannedAt) {
        return planOf(pipelineId, execution, planned, plannedAt, Map.of(), Set.of());
    }

    /**
     * The plan a run is submitted on: which run it is, the members its widths were worked out for, and each node's
     * width and batch as {@code planned} worked them out - with, for each sink in {@code sinkConnectors}, what it
     * holds open and buffers at that width, its writers sharing a connector per member only where
     * {@code sharedConnectors} names the connector it opens.
     */
    static ExecutionPlan planOf(String pipelineId, PipelineActuationOwnership.Execution execution,
            DagSource.PlannedDag planned, Instant plannedAt, Map<String, String> sinkConnectors,
            Set<String> sharedConnectors) {
        ExecutionFence fence = execution.fence();
        Map<String, Integer> processorsByVertex = new HashMap<>();
        planned.vertices().forEach((node, vertices) -> {
            NodeParallelism parallelism = planned.shape().nodes().get(node);
            if (parallelism != null) {
                vertices.forEach(vertex -> processorsByVertex.put(vertex, parallelism.effective()));
            }
        });
        List<ExecutionPlan.Node> nodes = new ArrayList<>();
        planned.shape().nodes().forEach((node, parallelism) -> {
            BatchSpec batch = planned.batches().getOrDefault(node, BatchSpec.DEFAULTS);
            ExecutionPlan.Node planning = ExecutionPlan.Node.of(parallelism, batch.effectiveMaxRecords(),
                    batch.effectiveMaxWaitMillis(), planned.vertices().getOrDefault(node, List.of()));
            String connector = sinkConnectors.get(node);
            nodes.add(connector == null ? planning : planning.withResources(PlannedSinkResources.of(parallelism,
                    batch.effectiveMaxRecords(), sharedConnectors.contains(connector),
                    planned.feeding().getOrDefault(node, List.of()), processorsByVertex)));
        });
        return new ExecutionPlan(pipelineId, fence == null || fence.claimGeneration() == 0
                ? null : fence.claimGeneration(),
                fence == null ? null : fence.executionGeneration(), execution.topologyRevision(), planned.members(),
                nodes, plannedAt);
    }
}
