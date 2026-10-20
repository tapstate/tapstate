package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
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

/** One fixed convergence command and its captured concrete job, never a later same-name execution. */
final class ScopedEngineLifecycleActuator implements LifecycleActuator {
    @FunctionalInterface interface Operators {
        void prepare(OperatorRunManifest original, Collection<OperatorMapCapacity> capacities,
                Collection<String> members, Runnable requireActive);
    }
    record Ports(Operators operators, Consumer<OperatorRunManifest> releaseHot,
            BiConsumer<PipelineConvergencePass,Optional<OperatorRunManifest>> rebuild,
            BiFunction<ExecutionFence,WorkloadClaimFence,ExecutionPlanRecorder> plans) {
        Ports {
            Objects.requireNonNull(operators,"operators"); Objects.requireNonNull(releaseHot,"releaseHot");
            Objects.requireNonNull(rebuild,"rebuild"); Objects.requireNonNull(plans,"plans");
        }
    }
    /** At most one unresolved submission per Pipeline in this assembly; no absence is inferred from a timeout. */
    static final class Submissions {
        private final java.util.concurrent.ConcurrentMap<String,OperatorRunManifest> pending = new java.util.concurrent.ConcurrentHashMap<>();
        void attempting(OperatorRunManifest original) {
            if (pending.putIfAbsent(original.pipelineId(),original) != null) throw fenced();
        }
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

    ScopedEngineLifecycleActuator(PipelineConvergencePass pass,Engine engine,DagSource dags,
            PipelineCaptureCoordinator captures,PipelineActuationOwnership ownership,String defaultDatabase,
            ConnectorReadiness connectors,Clock clock,Ports ports,Submissions submissions) {
        this.pass = Objects.requireNonNull(pass,"pass"); this.engine = Objects.requireNonNull(engine,"engine");
        this.dags = Objects.requireNonNull(dags,"dags"); this.captures = Objects.requireNonNull(captures,"captures");
        this.ownership = Objects.requireNonNull(ownership,"ownership"); this.defaultDatabase = Objects.requireNonNull(defaultDatabase,"defaultDatabase");
        this.connectors = Objects.requireNonNull(connectors,"connectors"); this.clock = Objects.requireNonNull(clock,"clock");
        this.ports = Objects.requireNonNull(ports,"ports"); this.submissions = Objects.requireNonNull(submissions,"submissions");
        this.command = pass.claim(); require();
        run = engine.captureRun(id()).orElse(null); require();
        if (run != null) {
            manifest = run.operatorManifest();
            if (!(manifest.executionIdentity() instanceof ExecutionFence fixed)) throw fenced();
            requireEarlier(fixed); original = fixed;
        } else if (command.executionGeneration() > 0) original = new ExecutionFence(pass.scope(),command);
        submissions.observed(id(),manifest); require();
        if (original != null && !engine.isLost()) capture = boundCapture(original);
        require();
    }

    private String id() { return pass.scope().pipelineId(); }
    private void requireId(String pipeline) { if (!id().equals(pipeline)) throw fenced(); require(); }
    private void require() {
        if (invalidated) throw fenced();
        pass.requirePipeline(id());
        if (!command.equals(pass.claim()) || !ownership.proveClaim(pass.scope(),command)) throw fenced();
        pass.requirePipeline(id());
        if (!command.equals(pass.claim())) throw fenced();
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
        require(); var bound = Objects.requireNonNull(captures.forRun(pass.scope(),command,fixed),"fixed capture view"); require(); return bound;
    }

    @Override public synchronized void start(String pipeline) {
        requireId(pipeline); engine.refuseIfLost(id());
        DagSource.StartPreparation prepared;
        Set<String> shared;
        try {
            prepared = dags.prepareStart(pass,defaultDatabase); require();
            if (prepared.operatorPlan().isEmpty() || prepared.artifactSnapshot().isEmpty() || prepared.sourceModels().isEmpty()) throw fenced();
            shared = Set.copyOf(connectors.requireEveryMemberCanLoad(id(),Set.copyOf(prepared.sinkConnectors().values()))); require();
        } catch (TapstateException refused) {
            ownership.startRefusedBeforeItsRun(pass.scope(),command); throw refused;
        }
        stopOriginal(); require();
        var execution = ownership.beginExecution(pass.scope(),command);
        if (!execution.allowed()) throw fenced();
        pass.advanced(execution.claimFence()); command = execution.claimFence(); require();
        original = execution.fence(); manifest = prepared.operatorPlan().orElseThrow().bind(original);
        run = null; capture = boundCapture(original);
        boolean submissionAttempted = false;
        try {
            pass.prepareOperatorDatabases(command,
                    prepared.operatorPlan().orElseThrow().nestDatabases().values(), this::require); require();
            var planned = prepared.plan(); require();
            var members = planned.members();
            ports.operators().prepare(manifest,prepared.operatorPlan().orElseThrow().capacities(),members,this::require); require();
            try { capture.startCapture(id(),prepared.artifactSnapshot().orElseThrow(),prepared.sourceModels().orElseThrow()); }
            catch (RingNotOpenYet waiting) { return; }
            require(); pass.bindCapture(command,capture.captureBindings()); require();
            var built = planned.build(original); require();
            if (!members.equals(built.planned().members())) throw new IllegalStateException("topology changed its prepared members");
            var recorder = Objects.requireNonNull(ports.plans().apply(original,command),"fixed plan recorder"); require();
            var previous = recorder.last(id()); require();
            recorder.record(EngineLifecycleActuator.planOf(id(),execution,built.planned(),clock.instant(),prepared.sinkConnectors(),shared).replacing(previous)); require();
            submissions.attempting(manifest); submissionAttempted = true;
            run = engine.submitRun(id(),built.dag(),prepared.capacity().mapDatabases(),prepared.capacity().settings(),manifest);
            submissions.returned(manifest); require();
        } catch (RuntimeException | Error failed) {
            if (submissionAttempted && run == null) {
                // The original writer may exist. A later pass must observe this exact manifest before proceeding.
                invalidated = true;
            } else {
                try { cleanupFailedStart(); } catch (RuntimeException | Error cleanup) { if (cleanup != failed) failed.addSuppressed(cleanup); }
            }
            throw failed;
        }
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

    private void stopOriginal() {
        require();
        if (run != null) {
            run.cancel(); require();
            if (!run.awaitTerminal(STOP_BUDGET)) throw new TapstateException(EngineError.JOB_STILL_ENDING,
                    Map.of("pipeline",id(),"seconds",STOP_BUDGET.toSeconds()),null);
            require();
        }
        if (capture != null) { capture.stopCapture(id(),false); require(); }
        if (manifest != null && !engine.isLost()) { ports.releaseHot().accept(manifest); require(); }
        if (original != null) { ports.plans().apply(original, command).forget(id()); require(); }
    }

    @Override public synchronized void stop(String pipeline,boolean purge) {
        requireId(pipeline);
        try { stopOriginal(); } finally { if (purge) invalidated = true; }
        if (purge) {
            ports.rebuild().accept(pass,Optional.ofNullable(manifest));
            // A changed physical generation must be loaded by a new pass, never adopted by this command.
            throw new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);
        }
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
    @Override public synchronized Optional<Throwable> failure(String pipeline) {
        requireId(pipeline);
        if (engine.isLost()) { var lost = engine.lost(id()).map(Throwable.class::cast); require(); return lost; }
        Optional<Throwable> failure = run != null && isCurrentRun() ? run.failure() : Optional.empty(); require();
        if (failure.isPresent() || capture == null) return failure;
        var captured = capture.captureFailure(id()); require(); return captured;
    }
    @Override public synchronized Optional<Throwable> lost(String pipeline) {
        requireId(pipeline); var lost = engine.lost(id()).map(Throwable.class::cast); require(); return lost;
    }
    @Override public synchronized boolean isCarryingAJob(String pipeline) {
        requireId(pipeline); boolean carrying = run != null && isCurrentRun() && run.isCarryingAJob(); require(); return carrying;
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
}
