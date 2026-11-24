package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.OperatorMapCapacity;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope.Phase;

/** One fixed convergence command and its captured concrete job, never a later same-name execution. */
final class ScopedEngineLifecycleActuator implements LifecycleActuator {
    @FunctionalInterface interface Operators {
        void prepare(OperatorRunManifest original, Collection<OperatorMapCapacity> capacities,
                Collection<String> members, Runnable requireActive);
    }
    record Ports(Operators operators, Consumer<OperatorRunManifest> releaseHot,
            BiConsumer<PipelineConvergencePass,Optional<OperatorRunManifest>> rebuild,
            BiFunction<ExecutionFence,WorkloadClaimFence,ExecutionPlanRecorder> plans,
            java.util.function.Predicate<ExecutionFence> capturePreparationLease, PipelineConnectorExecutions connectorExecutions) {
        Ports(Operators operators, Consumer<OperatorRunManifest> releaseHot,
                BiConsumer<PipelineConvergencePass,Optional<OperatorRunManifest>> rebuild,
                BiFunction<ExecutionFence,WorkloadClaimFence,ExecutionPlanRecorder> plans) {
            this(operators, releaseHot, rebuild, plans, ignored -> false, null);
        }
        Ports(Operators operators, Consumer<OperatorRunManifest> releaseHot,
                BiConsumer<PipelineConvergencePass,Optional<OperatorRunManifest>> rebuild,
                BiFunction<ExecutionFence,WorkloadClaimFence,ExecutionPlanRecorder> plans,
                java.util.function.Predicate<ExecutionFence> capturePreparationLease) {
            this(operators, releaseHot, rebuild, plans, capturePreparationLease, null);
        }
        Ports {
            Objects.requireNonNull(operators,"operators"); Objects.requireNonNull(releaseHot,"releaseHot");
            Objects.requireNonNull(rebuild,"rebuild"); Objects.requireNonNull(plans,"plans");
            Objects.requireNonNull(capturePreparationLease,"capturePreparationLease");
        }
    }
    /** At most one unresolved submission per Pipeline in this assembly; no absence is inferred from a timeout. */
    static final class Submissions {
        private final java.util.concurrent.ConcurrentMap<String,OperatorRunManifest> pending = new java.util.concurrent.ConcurrentHashMap<>();
        void attempting(OperatorRunManifest original) {
            if (pending.putIfAbsent(original.pipelineId(),original) != null) throw fenced();
        }
        boolean hasPending(String pipeline) { return pending.containsKey(pipeline); }
        void returned(OperatorRunManifest original) { pending.remove(original.pipelineId(),original); }
        void observed(String pipeline,OperatorRunManifest actual) {
            var original = pending.get(pipeline);
            if (original == null) return;
            if (!original.equals(actual)) throw fenced();
            pending.remove(pipeline,original);
        }
    }
    private static final Duration STOP_BUDGET = Duration.ofSeconds(30);
    private final PipelineConvergencePass pass;
    private final Engine engine;
    private final DagSource dags;
    private final PipelineCaptureCoordinator captures;
    private final PipelineActuationOwnership ownership;
    private final String defaultDatabase;
    private final ConnectorReadiness connectors;
    private final Clock clock;
    private final Ports ports;
    private final Submissions submissions;
    private boolean invalidated;
    private WorkloadClaimFence command;
    private Engine.Run run;
    private OperatorRunManifest manifest;
    private ExecutionFence original;
    private PipelineCaptureCoordinator capture;
    private boolean checkingStart;
    private boolean recoveringSource;

    ScopedEngineLifecycleActuator(PipelineConvergencePass pass,Engine engine,DagSource dags,
            PipelineCaptureCoordinator captures,PipelineActuationOwnership ownership,String defaultDatabase,
            ConnectorReadiness connectors,Clock clock,Ports ports,Submissions submissions) {
        this.pass = Objects.requireNonNull(pass,"pass"); this.engine = Objects.requireNonNull(engine,"engine");
        this.dags = Objects.requireNonNull(dags,"dags"); this.captures = Objects.requireNonNull(captures,"captures");
        this.ownership = Objects.requireNonNull(ownership,"ownership"); this.defaultDatabase = Objects.requireNonNull(defaultDatabase,"defaultDatabase");
        this.connectors = Objects.requireNonNull(connectors,"connectors"); this.clock = Objects.requireNonNull(clock,"clock");
        this.ports = Objects.requireNonNull(ports,"ports"); this.submissions = Objects.requireNonNull(submissions,"submissions");
        this.command = pass.claim(); require();
        run = engine.captureRun(id()).orElse(null); requireLocal();
        if (run != null) {
            manifest = run.operatorManifest();
            if (!(manifest.executionIdentity() instanceof ExecutionFence fixed)) throw fenced();
            requireEarlier(fixed); original = fixed;
        } else if (command.executionGeneration() > 0) original = new ExecutionFence(pass.scope(),command);
        submissions.observed(id(),manifest);
        if (run != null && !isCurrentRun() && original.scope().equals(pass.scope())
                && command.executionGeneration() > 0 && captures instanceof ScopedPipelineCaptureCoordinator scoped) {
            require();
            var attempted = new ExecutionFence(pass.scope(), command);
            if (scoped.directRetry(attempted).isPresent() && run.status().isTerminal()) {
                // A failed replacement can leave its cancelled predecessor discoverable by name.
                // Its pending retry and partial capture belong to the attempted execution.
                run = null;
                manifest = null;
                original = attempted;
            }
        }
        if (original != null && !engine.isLost()) capture = boundCapture(original);
        requireLocal();
    }

    private String id() { return pass.scope().pipelineId(); }
    private void requireId(String pipeline) { if (!id().equals(pipeline)) throw fenced(); require(); }
    private void requireLocal() {
        if (invalidated) throw fenced();
        pass.requirePipeline(id());
        if (!command.equals(pass.claim())) throw fenced();
    }
    private void require() {
        requireLocal();
        boolean proved = checkingStart ? ownership.proveStartClaim(pass.scope(), command)
                : ownership.proveClaim(pass.scope(), command);
        if (!proved) throw fenced();
        requireLocal();
    }
    /** Preparation may use this process's confirmed lease; later reads and durable writes retain their fences. */
    private void requireCapturePreparation() {
        requireLocal();
        if (original != null && isCurrentRun() && ports.capturePreparationLease().test(original)) {
            requireLocal();
            return;
        }
        require();
    }
    private void requireEarlier(ExecutionFence fixed) {
        var scope = fixed.scope(); var current = pass.scope();
        if (scope == null || !scope.clusterId().equals(current.clusterId()) || scope.bindingVersion() != current.bindingVersion()
                || !scope.cacheIncarnation().equals(current.cacheIncarnation()) || !scope.pipelineId().equals(current.pipelineId())
                || scope.stateGeneration() > current.stateGeneration() || fixed.topologyRevision() > command.topologyRevision()
                || fixed.claimGeneration() > command.claimGeneration()
                || (fixed.claimGeneration() == command.claimGeneration() && (fixed.executionGeneration() > command.executionGeneration()
                        || !fixed.ownerNodeId().equals(command.owner().nodeId()) || !fixed.ownerBootId().equals(command.owner().bootId())))) throw fenced();
    }
    private boolean isCurrentRun() {
        return original != null && command.executionGeneration() > 0
                && original.equals(new ExecutionFence(pass.scope(),command));
    }
    private PipelineCaptureCoordinator boundCapture(ExecutionFence fixed) {
        requireLocal();
        var bound = Objects.requireNonNull(captures.forRun(pass.scope(),command,fixed),"fixed capture view");
        requireLocal();
        return bound;
    }

    private io.tapstate.core.common.FailureFact openingFailure;

    @Override public synchronized io.tapstate.core.common.FailureFact actuationFailureFact(String pipeline, Throwable failure) {
        // Reading a captured fact does not authorize another actuation after an uncertain submission.
        if (!id().equals(pipeline)) throw fenced();
        var fact = openingFailure;
        openingFailure = null;
        return fact != null && fact.cause() == failure ? fact : new io.tapstate.core.common.FailureFact(failure);
    }

    @Override public synchronized void start(String pipeline) {
        openingFailure = null;
        checkingStart = true;
        try {
            PipelineDiagnostics.measure(pipeline, Phase.PIPELINE_START, () -> { startOwned(pipeline); return null; });
        } finally {
            checkingStart = false;
        }
    }

    private void startOwned(String pipeline) {
        requireId(pipeline); engine.refuseIfLost(id());
        DagSource.StartPreparation prepared;
        Set<String> shared;
        try (var readiness = new PipelineStartWork<Set<String>>()) {
            try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_DAG_PREPARE)) {
                prepared = dags.prepareStart(pass, defaultDatabase, (ids, hashes) -> {
                    if (!ids.isEmpty() && (!hashes.isEmpty() || original == null || ports.connectorExecutions() == null)) {
                        readiness.tryStart(PipelineDiagnostics.branch(Phase.PIPELINE_CONNECTORS,
                                () -> checkConnectors(ids, hashes)));
                    }
                });
                requireLocal();
                if (prepared.operatorPlan().isEmpty() || prepared.artifactSnapshot().isEmpty() || prepared.sourceModels().isEmpty()) throw fenced();
            }
            shared = readiness.awaitOrRun(() -> PipelineDiagnostics.measure(id(), Phase.PIPELINE_CONNECTORS,
                    () -> {
                        var hashes = prepared.connectorHashes();
                        if (hashes.isEmpty() && original != null && ports.connectorExecutions() != null)
                            hashes = ports.connectorExecutions().retainedSinkHashes(pass.intent(), prepared.connectorNodes(), this::require);
                        return checkConnectors(Set.copyOf(prepared.sinkConnectors().values()), hashes);
                    }));
            requireLocal();
        } catch (TapstateException refused) {
            ownership.startRefusedBeforeItsRun(pass.scope(),command); throw refused;
        }
        var originalBeforeStart = original;
        try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_STOP_PREVIOUS)) {
            if (run != null || capture != null || manifest != null || original != null || pass.displacedExecution() > 0) {
                stopOriginal();
            }
        }
        PipelineActuationOwnership.Execution execution;
        try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_EXECUTION_CLAIM)) {
            execution = ownership.beginExecution(pass.scope(),command);
            if (!execution.allowed()) throw fenced();
            pass.advancedSubmission(execution); command = execution.claimFence(); requireLocal();
            original = execution.fence(); manifest = prepared.operatorPlan().orElseThrow().bind(original);
            if (captures instanceof ScopedPipelineCaptureCoordinator scoped) {
                if (recoveringSource) scoped.carryDirectRetries(originalBeforeStart, original);
                else scoped.clearDirectRetries(pass.scope());
            }
            run = null; capture = boundCapture(original);
            PipelineDiagnostics.execution(original);
        }
        boolean submissionAttempted = false;
        try {
            if (ports.connectorExecutions() != null)
                ports.connectorExecutions().prepare(pass.intent(), prepared.connectorNodes(), command, this::require);
            try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_OPERATOR_DATABASES)) {
                var databases = prepared.operatorPlan().orElseThrow().nestDatabases().values();
                if (!databases.isEmpty()) pass.prepareOperatorDatabases(command, databases, this::require);
            }
            var planned = prepared.plan(); requireLocal();
            var members = planned.members();
            Runnable openCapture;
            try (var captureInputs = new PipelineStartWork<Runnable>()) {
                java.util.function.Supplier<Runnable> prepareCapture = () -> capture.prepareCaptureStart(id(),
                        prepared.artifactSnapshot().orElseThrow(), prepared.sourceModels().orElseThrow());
                captureInputs.tryStart(PipelineDiagnostics.branch(Phase.PIPELINE_CAPTURE_INPUTS, prepareCapture));
                try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_OPERATOR_MAPS)) {
                    ports.operators().prepare(manifest, prepared.operatorPlan().orElseThrow().capacities(), members, this::require);
                }
                openCapture = captureInputs.awaitOrRun(() ->
                        PipelineDiagnostics.measure(id(), Phase.PIPELINE_CAPTURE_INPUTS, prepareCapture));
            }
            try {
                requireCapturePreparation();
                PipelineDiagnostics.measure(id(), Phase.PIPELINE_CAPTURE_START, () -> {
                    openCapture.run();
                    return null;
                });
            } catch (RingNotOpenYet waiting) {
                ports.releaseHot().accept(manifest);
                manifest = null;
                PipelineDiagnostics.event("waiting_for_capture_ring");
                return;
            }
            try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_CAPTURE_BIND)) {
                var bindings = capture.captureBindings();
                require();
                pass.bindCapture(command, bindings);
            }
            DagSource.StartPlan built;
            try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_DAG_BUILD)) { built = planned.build(original); requireLocal(); }
            if (!members.equals(built.planned().members())) throw new IllegalStateException("topology changed its prepared members");
            if (ports.connectorExecutions() != null)
                ports.connectorExecutions().publish(pass.intent(), prepared.connectorNodes(), command, this::require);
            try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_PLAN_RECORD)) {
                var recorder = Objects.requireNonNull(ports.plans().apply(original,command),"fixed plan recorder");
                var previous = recorder.last(id());
                var plan = EngineLifecycleActuator.planOf(id(),execution,built.planned(),clock.instant(),prepared.sinkConnectors(),shared)
                        .replacing(previous);
                require();
                recorder.record(plan);
            }
            checkingStart = false;
            try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_JET_SUBMIT)) {
                require();
                submissions.attempting(manifest); submissionAttempted = true;
                run = engine.submitRun(id(),built.dag(),prepared.capacity().mapDatabases(),prepared.capacity().settings(),manifest);
                submissions.returned(manifest); require();
                PipelineDiagnostics.job(run.jobId());
                PipelineDiagnostics.event("jet_submission_returned");
            }
        } catch (RuntimeException | Error failed) {
            if (!(failed instanceof VirtualMachineError) && capture != null) {
                openingFailure = capture.openingFailureFact(pipeline, failed);
            }
            if (submissionAttempted && run == null) {
                // The original writer may exist. A later pass must observe this exact manifest before proceeding.
                invalidated = true;
            } else {
                try {
                    cleanupFailedStart();
                    if (ports.connectorExecutions() != null)
                        ports.connectorExecutions().abandon(pass.intent(), command, this::require);
                } catch (RuntimeException | Error cleanup) { if (cleanup != failed) failed.addSuppressed(cleanup); }
            }
            throw failed;
        }
    }

    private Set<String> checkConnectors(Set<String> ids, java.util.Map<String, String> hashes) {
        return Set.copyOf(hashes.isEmpty() ? connectors.requireEveryMemberCanLoad(id(), ids)
                : connectors.requireEveryMemberCanLoad(id(), ids, hashes));
    }

    /** Failed-start cleanup uses only the handles captured by that start, even after command revocation. */
    private void cleanupFailedStart() {
        boolean over = run == null;
        if (run != null) { run.cancel(); over = run.awaitTerminal(STOP_BUDGET); }
        if (!over || engine.isLost()) return;
        if (capture != null) capture.stopCapture(id(),false);
        if (manifest != null) ports.releaseHot().accept(manifest);
        if (original != null) ports.plans().apply(original, command).forget(id());
    }

    private void stopOriginal() { stopOriginal(false); }

    private void stopOriginal(boolean trackOnStopWorker) {
        require();
        if (run != null) {
            stopStep(trackOnStopWorker, "Engine.RunHandle.cancel", run::cancel);
            require();
            stopStep(trackOnStopWorker, "Engine.RunHandle.awaitTerminal", () -> {
                boolean ended = trackOnStopWorker ? run.awaitTerminal() : run.awaitTerminal(STOP_BUDGET);
                if (!ended) throw new TapstateException(EngineError.JOB_STILL_ENDING,
                        Map.of("pipeline",id(),"seconds",STOP_BUDGET.toSeconds()),null);
            });
            require();
        }
        if (run == null && pass.displacedExecution() > 0 && captures instanceof ScopedPipelineCaptureCoordinator scoped) {
            // An absent Jet handle cannot supply the old capture fence. The successor may retire only
            // local sessions in the original scope at or below its admitted fence.
            stopStep(trackOnStopWorker, "ScopedPipelineCaptureCoordinator.retireScope",
                    () -> scoped.retireScope(pass.scope(), command));
            require();
        } else if (capture != null) {
            if (trackOnStopWorker) {
                stopStep(true, "PipelineCaptureCoordinator.stopCapture", this::awaitCaptureClose);
                require();
            } else if (capture.stopCaptureIfPresent(id(), false) || !checkingStart) require();
        }
        if (manifest != null && !engine.isLost()) {
            stopStep(trackOnStopWorker, "releaseHot", () -> ports.releaseHot().accept(manifest));
            require();
        }
        if (original != null) {
            if (ports.plans().apply(original, command).forgetIfPresent(id()) || !checkingStart) require();
        }
    }

    private void awaitCaptureClose() {
        for (;;) {
            require();
            try {
                capture.stopCapture(id(), false);
                return;
            } catch (io.tapstate.spi.capture.SnapshotSession.PendingClose pending) {
                awaitOriginalClose(pending);
            } catch (TapstateException pending) {
                boolean captureStillEnding = pending.code() == io.tapstate.runtime.srs.CaptureError.STILL_ENDING;
                boolean connectorStillEnding = pending.code() == io.tapstate.adapters.pdk.ConnectorError.CAPTURE_FAILED
                        && pending.getCause() instanceof java.util.concurrent.TimeoutException;
                if (Thread.currentThread().isInterrupted() || (!captureStillEnding && !connectorStillEnding)) throw pending;
                try { Thread.sleep(100); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw pending;
                }
            }
        }
    }

    private static void awaitOriginalClose(io.tapstate.spi.capture.SnapshotSession.PendingClose pending) {
        try {
            pending.completion().toCompletableFuture().get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw pending;
        } catch (java.util.concurrent.ExecutionException failure) {
            if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
            if (failure.getCause() instanceof Error error) throw error;
            throw pending;
        }
    }

    private void stopStep(boolean track, String operation, Runnable step) {
        if (!track) { step.run(); return; }
        String node = operation.equals("Engine.RunHandle.awaitTerminal") ? "unknown" : command.owner().nodeId();
        var site = new io.tapstate.runtime.engine.StopWaitLog.Site(id(),
                run == null ? Long.toString(command.executionGeneration()) : Long.toString(run.jobId()), node, operation);
        io.tapstate.runtime.engine.StopWaitLog.around(site, step);
    }

    @Override public void stop(String pipeline,boolean purge) {
        requireId(pipeline);
        if (captures instanceof ScopedPipelineCaptureCoordinator scoped) scoped.clearDirectRetries(pass.scope());
        boolean requestedStop = pass.intent().desired().targetState() == PipelineState.STOPPED;
        try {
            stopOriginal(requestedStop);
            if (ports.connectorExecutions() != null) ports.connectorExecutions().stopped(pass.intent(), command, this::require);
        } finally { if (purge) invalidated = true; }
        if (purge) rebuildStopped();
    }

    @Override public void clearStopped(String pipeline) {
        requireId(pipeline);
        invalidated = true;
        rebuildStopped();
    }

    private void rebuildStopped() {
        ports.rebuild().accept(pass, Optional.ofNullable(manifest));
        // A changed physical generation must be loaded by a new pass, never adopted by this command.
        throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
    }
    @Override public synchronized void pause(String pipeline) {
        requireId(pipeline); if (run == null || !isCurrentRun()) throw fenced(); run.suspend(); require();
    }
    @Override public synchronized void resume(String pipeline) {
        requireId(pipeline);
        if (!isCurrentRun() || run == null || capture == null || !capture.loadDelivered(id())) {
            require(); start(id()); return;
        }
        require(); run.resume(); require();
    }
    @Override public synchronized Optional<SourceRecovery> recoverSource(
            String pipeline, java.util.function.BooleanSupplier currentIntent) {
        requireId(pipeline);
        if (original == null || !isCurrentRun() || !(captures instanceof ScopedPipelineCaptureCoordinator scoped)) {
            return Optional.empty();
        }
        var episode = scoped.directRetry(original);
        if (episode.isEmpty()) return Optional.empty();
        var retry = episode.orElseThrow();
        var pending = retry.decision(scoped.streamRetryPolicy());
        if (pending == null) return Optional.empty();
        if (pending.delay() == null) {
            scoped.clearDirectRetries(pass.scope());
            return Optional.of(new SourceRecovery(pending.failure()));
        }
        if (!pending.delay().isZero() || !currentIntent.getAsBoolean()) return Optional.of(new SourceRecovery(null));
        retry.attempting();
        recoveringSource = true;
        try {
            start(pipeline);
            // A shared source can defer submission before the replacement Direct reader is created.
            if (run == null) retry.interrupted(pending.failure());
            return Optional.of(new SourceRecovery(null));
        } catch (TapstateException failure) {
            if (invalidated) throw failure;
            if (original != null && scoped.hasDirectRetryFor(original, failure)) return Optional.of(new SourceRecovery(null));
            scoped.clearDirectRetries(pass.scope());
            return Optional.of(new SourceRecovery(actuationFailureFact(pipeline, failure)));
        } finally {
            recoveringSource = false;
        }
    }

    @Override public synchronized Optional<Throwable> terminalLeaseFailure(String pipeline) {
        requireId(pipeline);
        return pass.renewalFailure().map(Throwable.class::cast);
    }

    @Override public synchronized Optional<LeaseLoss> leaseLoss(String pipeline) {
        long displaced = pass.displacedExecution();
        if (displaced > 0 && displaced == command.executionGeneration()
                && (run == null || original != null && pass.scope().equals(original.scope()) && !run.completed())) {
            requireId(pipeline);
            return Optional.of(new LeaseLoss(displaced,
                    new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", pipeline), null)));
        }
        if (run == null || original == null || !pass.scope().equals(original.scope())) return Optional.empty();
        if (run.isCarryingAJob()) {
            if (original.claimGeneration() >= command.claimGeneration()) return Optional.empty();
            requireId(pipeline);
            // The successor can fence this old execution even before its Jet tasks notice the loss.
            return Optional.of(new LeaseLoss(original.executionGeneration(),
                    new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", pipeline), null)));
        }
        var failure = run.failure();
        for (Throwable cause = failure.orElse(null); cause != null; cause = cause.getCause()) {
            if (cause instanceof TapstateException coded && coded.code() == EngineError.EXECUTION_NOT_AUTHORIZED) {
                requireId(pipeline);
                return Optional.of(new LeaseLoss(original.executionGeneration(), coded));
            }
        }
        return Optional.empty();
    }

    @Override public synchronized boolean recoveredJobRunning(String pipeline) {
        requireId(pipeline);
        return run != null && isCurrentRun() && run.status() == com.hazelcast.jet.core.JobStatus.RUNNING;
    }

    @Override public synchronized Optional<Throwable> failure(String pipeline) {
        return failureFact(pipeline).map(io.tapstate.core.common.FailureFact::cause);
    }
    @Override public synchronized Optional<io.tapstate.core.common.FailureFact> failureFact(String pipeline) {
        requireId(pipeline);
        if (engine.isLost()) { var lost = engine.lost(id()).map(io.tapstate.core.common.FailureFact::new); require(); return lost; }
        Optional<io.tapstate.core.common.FailureFact> failure = run != null && isCurrentRun() ? run.failureFact() : Optional.empty(); require();
        // An earlier capture stays bound for cleanup, not for the current run's failure diagnosis.
        if (failure.isPresent() || capture == null || !isCurrentRun()) return failure;
        var captured = capture.captureFailureFact(id()); require(); return captured;
    }
    @Override public synchronized Optional<Throwable> lost(String pipeline) {
        requireId(pipeline); var lost = engine.lost(id()).map(Throwable.class::cast); require(); return lost;
    }
    @Override public synchronized boolean isCarryingAJob(String pipeline) {
        requireId(pipeline);
        // A successful bounded job still owns completion while its target receipts arrive.
        boolean carrying = (run != null && isCurrentRun() && run.isCarryingAJob()) || successfulBoundedJob(pipeline);
        require();
        return carrying;
    }
    @Override public synchronized void settleCompleted(String pipeline) { retryCompletion(pipeline); }
    @Override public synchronized boolean completed(String pipeline) {
        requireId(pipeline);
        if (!successfulBoundedJob(pipeline)) {
            return false;
        }
        boolean complete;
        try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_TARGET_CONFIRM)) {
            complete = capture.loadDelivered(pipeline) && capture.captureFailure(pipeline).isEmpty() && run.failure().isEmpty();
            require();
        }
        if (complete) PipelineDiagnostics.event("bounded_confirmation_observed");
        return complete;
    }

    private boolean successfulBoundedJob(String pipeline) {
        return run != null && isCurrentRun() && run.completed() && capture != null
                && capture.isCapturing(pipeline) && !capture.captureBindings().snapshots().isEmpty();
    }
    @Override public synchronized Optional<Completion> completion(String pipeline) {
        if (!completed(pipeline)) return Optional.empty();
        var proof = new Completion(Long.toString(run.jobId()),command); require(); return Optional.of(proof);
    }
    @Override public synchronized void complete(String pipeline) {
        complete(pipeline,completion(pipeline).orElseThrow(ScopedEngineLifecycleActuator::fenced));
    }
    @Override public synchronized void complete(String pipeline,Completion proof) {
        requireId(pipeline); Objects.requireNonNull(proof,"completion");
        if (run==null || !isCurrentRun() || !run.completed() || !Long.toString(run.jobId()).equals(proof.executionId())
                || !command.equals(proof.fence())) throw fenced();
        releaseCompletedOriginal();
    }
    @Override public synchronized void retryCompletion(String pipeline) {
        requireId(pipeline);
        // The persisted terminal result authorizes retrying exact cleanup, not cancelling a live run.
        // A prior partial cleanup may already have removed its capture and target-ACK registration.
        if (run!=null && run.isCarryingAJob()) throw fenced();
        releaseCompletedOriginal();
    }
    private void releaseCompletedOriginal() {
        require();
        if (run!=null && run.isCarryingAJob()) throw fenced();
        if (capture!=null) { capture.stopCapture(id(),false); require(); }
        if (manifest!=null && !engine.isLost()) { ports.releaseHot().accept(manifest); require(); }
        if (original!=null) { ports.plans().apply(original,command).forget(id()); require(); }
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
}
