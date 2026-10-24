package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.OperatorMapBindings;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;

/** Preflights and publishes one original shared replacement without starting any affected Pipeline. */
final class NativeSharedSrsRecovery implements AutoCloseable {
    private record Finalized(StorageRecoveryCommands.Plan plan,HazelcastOperatorReadiness.SharedTicket ticket) {}
    private final MongoConfigurationDomain configuration;
    private final MongoCacheDomain cache;
    private final CacheAuthScope auth;
    private final CacheBusinessScope business;
    private final Supplier<WorkloadClaimFence> node;
    private final Runnable active;
    private final Engine engine;
    private final EngineLifecycleActuator actuator;
    private final HazelcastOperatorReadiness operators;
    private final HazelcastExecutionPlans plans;
    private final ScopedPipelineCaptureCoordinator captures;
    private final HazelcastInstance member;
    private final SharedSrsRecoveryInspector inspector;
    private final SharedSrsPreviewMember participant;
    private volatile Finalized finalized;

    NativeSharedSrsRecovery(MongoConfigurationDomain configuration,MongoCacheDomain cache,CacheAuthScope auth,
            PipelineActuationOwnership ownership,Supplier<WorkloadClaimFence> node,Runnable active,Engine engine,
            EngineLifecycleActuator actuator,HazelcastOperatorReadiness operators,HazelcastExecutionPlans plans,
            ScopedPipelineCaptureCoordinator captures,HazelcastInstance member) {
        this.configuration=Objects.requireNonNull(configuration);this.cache=Objects.requireNonNull(cache);this.auth=Objects.requireNonNull(auth);
        this.business=new CacheBusinessScope(auth.clusterId(),auth.bindingVersion(),auth.cacheIncarnation());
        this.node=Objects.requireNonNull(node);this.active=Objects.requireNonNull(active);this.engine=Objects.requireNonNull(engine);
        this.actuator=Objects.requireNonNull(actuator);this.operators=Objects.requireNonNull(operators);this.plans=Objects.requireNonNull(plans);
        this.captures=Objects.requireNonNull(captures);this.member=Objects.requireNonNull(member);
        inspector=new SharedSrsRecoveryInspector(configuration,cache,business,active);
        participant=new SharedSrsPreviewMember(configuration.recoveryCommands(),auth,ownership,cache.workloadClaims(business),
                () -> node.get().owner(),configuration::configurationRevision,active,this::requireLocallyIsolated);
        HazelcastOperatorReadiness.bindSharedPreview(member,participant,captures);
    }

    StorageRecoveryCommands.Manifest inspect(StorageRecoveryCommands.Selection selection){return inspector.inspect(selection);}
    void requirePlanningCurrent(StorageRecoveryCommands.Manifest original){active.run();inspector.requireConfiguration(original);}

    synchronized StorageRecoveryCommands.Plan prepared(StorageRecoveryCommands.Plan original,Runnable caller) {
        requireAttempt(original);if(original.phase()!=StorageRecoveryCommands.Phase.PLANNED)throw fenced();
        finalized=null;Runnable fixed=() -> {caller.run();requireAttempt(original);inspector.requireConfiguration(original.manifest());};
        var ticket=quiesce(original,fixed);ticket.verifyEnded(fixed);
        var observed=observe(original);requireSameScopes(original,observed);
        var verified=inspector.manifest(original.manifest().selection(),observed,original.controlRevision());
        StorageRecoveryCommands.Plan result;
        if(sameClosure(original.manifest(),verified))result=original;
        else {
            var cancelled=cancelled(original);
            Runnable local=() -> {caller.run();ticket.requireVerified(original,active);inspector.requireConfiguration(verified);};
            result=cancelled.isPresent()?configuration.sharedRecoveryPreparations().guardedBy(local).replan(cancelled.orElseThrow(),verified)
                    :configuration.recoveryCommands().guardedBy(local).plan(auth,verified,original.operationId(),null);
        }
        finalizeExact(result,caller);return result;
    }

    /** Resolving this hook is read-only. A cancellation or replan runs only inside the service's original audit. */
    Optional<Supplier<StorageRecoveryCommands.Plan>> replanning(StorageRecoveryCommands.Plan original,Runnable caller) {
        active.run();requireAuth(original);
        if(original.phase()==StorageRecoveryCommands.Phase.RUNNING)return Optional.of(() -> replanRunning(original,caller));
        if(original.phase()==StorageRecoveryCommands.Phase.PLANNED || original.phase()==StorageRecoveryCommands.Phase.CONFIRMED) {
            var receipt=cancelled(original);
            if(receipt.isPresent())return Optional.of(() -> replanCancelled(original,receipt.orElseThrow(),caller));
        }
        return Optional.empty();
    }

    private synchronized StorageRecoveryCommands.Plan replanRunning(StorageRecoveryCommands.Plan original,Runnable caller) {
        caller.run();requireAttempt(original);
        var pending=configuration.sharedRecoveryPreparations().pending(auth,original.operationId()).orElseThrow(NativeSharedSrsRecovery::fenced);
        var physical=cache.sharedSrsRecoveries(business,active);var applied=physical.applied(pending);
        if(applied.isPresent())return complete(applied.orElseThrow());
        var result=physical.cancel(pending,node.get());caller.run();
        if(result.outcome()==SharedSrsRecoveryReceipt.Outcome.RETIRED)return complete(result);
        return replanCancelled(original,result,caller);
    }

    private synchronized StorageRecoveryCommands.Plan replanCancelled(StorageRecoveryCommands.Plan original,SharedSrsRecoveryReceipt receipt,Runnable caller) {
        caller.run();requireAttempt(original);finalized=null;
        var observed=receipt.cancelledSnapshot();inspector.requireRest(observed);
        long revision=configuration.configurationRevision();
        var manifest=inspector.manifest(original.manifest().selection(),observed,revision);inspector.requireConfiguration(manifest);
        return configuration.sharedRecoveryPreparations().guardedBy(() -> {caller.run();inspector.requireConfiguration(manifest);}).replan(receipt,manifest);
    }

    synchronized void verifyExternal(StorageRecoveryCommands.Manifest manifest,Runnable caller) {
        var current=configuration.recoveryCommands().current(auth).orElseThrow(NativeSharedSrsRecovery::fenced);
        if(!current.manifest().equals(manifest))throw fenced();finalizeExact(current,caller);
    }

    /** Local finalized ACK plus CONFIG dependencies; original caller authentication is retained by the service. */
    void requireFinalized(StorageRecoveryCommands.Manifest manifest) {
        active.run();var proof=finalized;
        if(proof==null || !proof.plan().manifest().equals(manifest))throw fenced();
        proof.ticket().requireVerified(proof.plan(),active);inspector.requireConfiguration(manifest);
    }

    boolean isCurrent(StorageRecoveryCommands.Plan original) {
        try {
            requireAuth(original);
            if(original.phase()==StorageRecoveryCommands.Phase.COMPLETE || original.phase()==StorageRecoveryCommands.Phase.RUNNING)return true;
            requireAttempt(original);requireFinalized(original.manifest());return observe(original).equals(original.manifest().sharedSrs());
        } catch(TapstateException unavailable){return false;}
    }

    synchronized void run(StorageRecoveryCommands.Plan requested) {
        active.run();requireAuth(requested);
        var current=configuration.recoveryCommands().find(auth,requested.operationId()).orElseThrow(NativeSharedSrsRecovery::fenced);
        if(!sameAttempt(requested,current))throw fenced();
        if(current.phase()==StorageRecoveryCommands.Phase.COMPLETE){maintain();return;}
        if(current.phase()==StorageRecoveryCommands.Phase.CONFIRMED) {
            finalizeExact(current,active);var receipt=cancelled(current);
            var originals=configuration.sharedRecoveryPreparations().guardedBy(() -> requireFinalized(current.manifest()));
            if(receipt.isPresent())originals.reserve(current,receipt.orElseThrow());else originals.reserve(current);
        } else if(current.phase()!=StorageRecoveryCommands.Phase.RUNNING)throw fenced();
        var pending=configuration.sharedRecoveryPreparations().pending(auth,current.operationId()).orElseThrow(NativeSharedSrsRecovery::fenced);
        var physical=cache.sharedSrsRecoveries(business,active);
        // A committed C is resumed from its own receipt before inspecting later authoring or old claims.
        var applied=physical.applied(pending);if(applied.isPresent()){complete(applied.orElseThrow());return;}
        try {
            var running=pending.plan();var ticket=quiesce(running,active);
            var guarded=cache.sharedSrsRecoveries(business,() -> ticket.requireStaged(running,active));
            complete(guarded.replace(pending,node.get()));
        } catch(TapstateException failed) {
            try {
                var result=physical.cancel(pending,node.get());
                if(result.outcome()==SharedSrsRecoveryReceipt.Outcome.RETIRED){complete(result);return;}
            } catch(TapstateException unknown){if(unknown!=failed)failed.addSuppressed(unknown);}
            throw failed;
        }
    }

    private void finalizeExact(StorageRecoveryCommands.Plan original,Runnable caller) {
        requireAttempt(original);
        if(original.phase()!=StorageRecoveryCommands.Phase.PLANNED && original.phase()!=StorageRecoveryCommands.Phase.CONFIRMED)throw fenced();
        Runnable fixed=() -> {caller.run();requireAttempt(original);inspector.requireConfiguration(original.manifest());};
        var ticket=quiesce(original,fixed);ticket.verifyEnded(fixed);
        if(!observe(original).equals(original.manifest().sharedSrs()))throw fenced();
        fixed.run();ticket.requireVerified(original,active);finalized=new Finalized(original,ticket);
    }

    private HazelcastOperatorReadiness.SharedTicket quiesce(StorageRecoveryCommands.Plan original,Runnable fixed) {
        fixed.run();inspector.requireRest(original.manifest().sharedSrs());var ticket=operators.freezeShared(original,fixed);
        Runnable held=() -> {fixed.run();ticket.requireHeld(original,active);};
        for(var pipeline:original.manifest().sharedSrs().pipelines().values())drain(pipeline.scope(),pipeline.claim(),held);
        held.run();ticket.stopCapture(held);ticket.endOwnedTenures(held);return ticket;
    }

    private void drain(PipelineIntentScope scope,WorkloadClaimFence ceiling,Runnable fixed) {
        fixed.run();var run=engine.captureRun(scope.pipelineId());
        actuator.requireResolvedSubmission(scope.pipelineId(),run.map(Engine.Run::operatorManifest));
        var manifests=new LinkedHashSet<OperatorRunManifest>();
        if(run.isPresent()) {
            var saved=run.orElseThrow();var manifest=saved.operatorManifest();
            if(!(manifest.executionIdentity() instanceof ExecutionFence original) || original.scope()==null)throw fenced();
            if(!scope.equals(original.scope())) {
                if(saved.isCarryingAJob())throw fenced();configuration.requireRetiredPipelinePredecessor(scope,original.scope());
            } else {
                ScopedPipelineCaptureCoordinator.requireNotLater(original,ceiling);
                if(saved.isCarryingAJob())throw fenced();
                if(!saved.awaitTerminal(Duration.ofSeconds(30)))throw new TapstateException(EngineError.JOB_STILL_ENDING,
                        Map.of("pipeline",scope.pipelineId(),"seconds",30),null);
                manifests.add(manifest);
            }
        }
        manifests.addAll(operators.isolate(scope,ceiling,fixed));
        for(var manifest:manifests) {
            fixed.run();operators.releaseHot(manifest);
            plans.forRun((ExecutionFence)manifest.executionIdentity(),fixed).forget(scope.pipelineId());
        }
        operators.requireRetired(scope,ceiling,fixed);fixed.run();
    }

    /** This callback must not take the producer monitor: it is also invoked by this member's RPC executor. */
    private void requireLocallyIsolated(PipelineIntentScope scope,WorkloadClaimFence ceiling) {
        active.run();var run=engine.captureRun(scope.pipelineId());
        actuator.requireResolvedSubmission(scope.pipelineId(),run.map(Engine.Run::operatorManifest));
        if(run.isPresent()) {
            var original=run.orElseThrow();if(original.isCarryingAJob() || !(original.operatorManifest().executionIdentity() instanceof ExecutionFence fence)
                    || fence.scope()==null)throw fenced();
            if(scope.equals(fence.scope()))ScopedPipelineCaptureCoordinator.requireNotLater(fence,ceiling);
            else configuration.requireRetiredPipelinePredecessor(scope,fence.scope());
        }
        if(captures.hasScope(scope) || !OperatorMapBindings.resolve(member,true).originals(scope).isEmpty())throw fenced();
    }

    private SharedSrsRecoverySnapshot observe(StorageRecoveryCommands.Plan original) {
        var receipt=cancelled(original);return receipt.isPresent()?receipt.orElseThrow().cancelledSnapshot():inspector.snapshot(original.manifest().sharedSrs().previous());
    }
    private Optional<SharedSrsRecoveryReceipt> cancelled(StorageRecoveryCommands.Plan original) {
        if(original.phase()==StorageRecoveryCommands.Phase.RUNNING || original.phase()==StorageRecoveryCommands.Phase.COMPLETE)return Optional.empty();
        try {
            var readable=configuration.capturePreparations().read(business,original.manifest().sharedSrs().previous().captureId());
            if(readable.isEmpty() || !readable.orElseThrow().equals(original.manifest().sharedSrs().previous()))throw fenced();
            return Optional.empty();
        } catch(TapstateException reserved) {
            var authority=configuration.sharedRecoveryPreparations().reconfirmation(auth,original.operationId());
            if(authority.isEmpty())throw reserved;
            // This sealed reconfirmation authority can only read an existing matching CANCELLED result.
            var receipt=cache.sharedSrsRecoveries(business,active).cancel(authority.orElseThrow(),node.get());
            if(receipt.outcome()!=SharedSrsRecoveryReceipt.Outcome.CANCELLED)throw fenced();return Optional.of(receipt);
        }
    }
    private StorageRecoveryCommands.Plan complete(SharedSrsRecoveryReceipt receipt) {
        var result=configuration.sharedRecoveryPreparations().guardedBy(active).complete(receipt);finalized=null;participant.maintain();return result;
    }
    private void requireAttempt(StorageRecoveryCommands.Plan original) {
        active.run();requireAuth(original);
        var current=configuration.recoveryCommands().find(auth,original.operationId()).orElseThrow(NativeSharedSrsRecovery::fenced);
        if(!sameAttempt(original,current) || current.phase()!=original.phase()
                || current.phase()!=StorageRecoveryCommands.Phase.RUNNING && current.phase()!=StorageRecoveryCommands.Phase.COMPLETE
                    && configuration.configurationRevision()!=current.controlRevision()+current.phase().ordinal())throw fenced();
    }
    private void requireAuth(StorageRecoveryCommands.Plan plan) {
        active.run();if(!auth.equals(plan.originalScope()) || plan.manifest().selection().extent()!=StorageRecoveryCommands.Extent.SHARED_SRS)throw fenced();
    }
    private static boolean sameAttempt(StorageRecoveryCommands.Plan left,StorageRecoveryCommands.Plan right) {
        return left.operationId().equals(right.operationId()) && left.originalScope().equals(right.originalScope())
                && Objects.equals(left.previousOperationId(),right.previousOperationId()) && left.planHash().equals(right.planHash())
                && left.controlRevision()==right.controlRevision() && left.manifest().equals(right.manifest());
    }
    private static boolean sameClosure(StorageRecoveryCommands.Manifest left,StorageRecoveryCommands.Manifest right) {
        return left.selection().equals(right.selection()) && left.definitionHashes().equals(right.definitionHashes())
                && left.dependencyHash().equals(right.dependencyHash()) && left.sharedSrs().equals(right.sharedSrs());
    }
    private static void requireSameScopes(StorageRecoveryCommands.Plan original,SharedSrsRecoverySnapshot observed) {
        if(!original.manifest().sharedSrs().previous().equals(observed.previous())
                || !original.manifest().sharedSrs().captureFence().equals(observed.captureFence())
                || !original.manifest().sharedSrs().pipelines().equals(observed.pipelines()))throw fenced();
    }
    @org.springframework.scheduling.annotation.Scheduled(fixedDelay=1000)
    void maintain(){participant.maintain();}
    @Override public void close(){finalized=null;participant.close();}
    private static TapstateException fenced(){return ThreeLayerStorePort.refused();}
}
