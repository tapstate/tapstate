package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;

/** Original lifecycle retirement behind the common artifact API, including a pending operation after restart. */
final class NativePipelineDeletion implements ArtifactMutationService.PipelineDeletion {
    private final MongoConfigurationDomain configuration;
    private final MongoCacheDomain cache;
    private final CacheBusinessScope business;
    private final PipelineActuationOwnership ownership;
    private final Supplier<WorkloadClaimFence> node;
    private final Runnable active;
    private final Engine engine;
    private final EngineLifecycleActuator actuator;
    private final HazelcastOperatorReadiness operators;
    private final HazelcastExecutionPlans plans;
    private final PipelineRetirements lifecycle;
    private final PipelineCacheRetirements retirement;
    private final SrsConsumerStore consumers;
    private final PipelinePrivateNoteRetirement notes;

    NativePipelineDeletion(MongoConfigurationDomain configuration, MongoCacheDomain cache, CacheBusinessScope business,
            PipelineActuationOwnership ownership, Supplier<WorkloadClaimFence> node, Runnable active,
            Engine engine, EngineLifecycleActuator actuator, HazelcastOperatorReadiness operators,
            HazelcastExecutionPlans plans, ConnectorProvisioner connectors) {
        this.configuration=configuration; this.cache=cache; this.business=business; this.ownership=ownership;
        this.node=node; this.active=active; this.engine=engine; this.actuator=actuator; this.operators=operators; this.plans=plans;
        lifecycle=configuration.retirements(); retirement=cache.pipelineRetirements(business,active);
        consumers=cache.srs(business,active).consumers();
        notes=new PipelinePrivateNoteRetirement(configuration.requiredNotes(),configuration.rebuilds(),connectors);
    }

    @Override public Runnable prepare(PipelineResource original, String expectedHash) {
        active.run(); var mapping=mapping(original.id()); var scope=scope(mapping);
        if (mapping.phase()==PipelineStateMapping.Phase.RETIRING) {
            var pending=lifecycle.pending(scope,mapping.operationId()).orElseThrow(NativePipelineDeletion::fenced);
            if (!pending.request().definitionHash().equals(CanonicalHash.of(original))) throw fenced();
        } else {
            if (mapping.phase()!=PipelineStateMapping.Phase.ACTIVE) throw fenced();
            requireRest(scope);
        }
        String operation=mapping.phase()==PipelineStateMapping.Phase.RETIRING?mapping.operationId():UUID.randomUUID().toString();
        return () -> delete(original,expectedHash,scope,operation);
    }

    private void delete(PipelineResource original, String expectedHash, PipelineIntentScope scope, String operation) {
        active.run(); var mapping=mapping(original.id());
        if (!scope.equals(scope(mapping)) || mapping.phase()==PipelineStateMapping.Phase.RETIRING && !operation.equals(mapping.operationId())) throw fenced();
        requireHash(original.id(),expectedHash);
        var pending=mapping.phase()==PipelineStateMapping.Phase.RETIRING
                ? lifecycle.pending(scope,mapping.operationId()) : Optional.<PipelineRetirementPreparation>empty();
        if (mapping.phase()!=PipelineStateMapping.Phase.ACTIVE && pending.isEmpty()) throw fenced();
        if (pending.isPresent() && !pending.orElseThrow().request().definitionHash().equals(expectedHash)) throw conflict(original.id());
        var preparation=configuration.preparations().read(original.id()).orElseThrow(NativePipelineDeletion::fenced);
        if (preparation.phase()==PipelinePreparation.Phase.ISSUED) throw fenced();
        boolean alreadyRetired=pending.isPresent() && pending.orElseThrow().request().cache().isPresent()
                && retirement.completed(pending.orElseThrow(),node.get()).isPresent();
        try (var control=alreadyRetired || preparation.phase()==PipelinePreparation.Phase.UNASSIGNED
                ? ownership.unpreparedControl(scope) : ownership.control(scope)) {
            control.requireHeld(); active.run();
            Optional<PipelineRetirementRequest.CacheOriginal> isolated=alreadyRetired ? pending.orElseThrow().request().cache()
                    : preparation.phase()==PipelinePreparation.Phase.UNASSIGNED ? Optional.empty()
                    : Optional.of(new PipelineRetirementRequest.CacheOriginal(control.claim(),state(scope).epoch()));
            var request=pending.map(PipelineRetirementPreparation::request).orElseGet(() ->
                    new PipelineRetirementRequest(operation,scope,expectedHash,isolated));
            var found=engine.captureRun(original.id());
            Optional<Engine.Run> predecessor=found.filter(run -> run.operatorManifest().executionIdentity() instanceof ExecutionFence fixed
                    && fixed.scope()!=null && fixed.scope().stateGeneration()<scope.stateGeneration());
            if (predecessor.isPresent()) {
                var previous=predecessor.orElseThrow();
                if (previous.isCarryingAJob()) throw fenced();
                configuration.requireRetiredPipelinePredecessor(scope,((ExecutionFence)previous.operatorManifest().executionIdentity()).scope());
            }
            var captured=predecessor.isPresent()?Optional.<Engine.Run>empty():found;
            var manifest=captured.map(Engine.Run::operatorManifest);
            actuator.requireResolvedSubmission(original.id(),manifest);
            WorkloadClaimFence ceiling=isolated.map(PipelineRetirementRequest.CacheOriginal::claim).orElse(null);
            if (manifest.isPresent()) {
                if (isolated.isEmpty() || !(manifest.orElseThrow().executionIdentity() instanceof ExecutionFence execution)
                        || !scope.equals(execution.scope())) throw fenced();
                ScopedPipelineCaptureCoordinator.requireNotLater(execution,ceiling);
            }
            Runnable proof=() -> {
                active.run(); control.requireHeld(); var current=mapping(original.id());
                if (current.stateGeneration()!=scope.stateGeneration()) throw fenced();
                if (current.phase()==PipelineStateMapping.Phase.ACTIVE) {
                    if (alreadyRetired) throw fenced();
                    control.requireActive(); requireRest(scope);
                } else if (current.phase()==PipelineStateMapping.Phase.RETIRING && request.operationId().equals(current.operationId())) {
                    var held=lifecycle.pending(scope,request.operationId()).orElseThrow(NativePipelineDeletion::fenced);
                    if (!held.request().equals(request)) throw fenced();
                    // Empty original history never has a CACHE receipt. The actual retirement still proves
                    // the live node and absence of unexpected history in its own CACHE transaction.
                    if (held.request().cache().isEmpty() || retirement.completed(held,node.get()).isEmpty()) {
                        control.requireActive(); requireRest(scope);
                    }
                } else throw fenced();
            };
            var coordinator=new PipelineDeletionCoordinator(lifecycle,retirement,consumers,notes,node,active,(fixed, ignored) -> {
                if (!fixed.equals(request)) throw fenced(); proof.run();
                if (captured.isPresent()) {
                    var run=captured.orElseThrow(); run.cancel(); proof.run();
                    if (!run.awaitTerminal(Duration.ofSeconds(30))) throw new TapstateException(EngineError.JOB_STILL_ENDING,
                            Map.of("pipeline",scope.pipelineId(),"seconds",30),null);
                }
                proof.run(); var retained=operators.isolate(scope,ceiling,proof);
                var all=new LinkedHashSet<OperatorRunManifest>(retained); manifest.ifPresent(all::add);
                for (var fixedManifest:all) {
                    proof.run(); if (alreadyRetired) operators.releaseRetiredHot(fixedManifest); else operators.releaseHot(fixedManifest);
                }
                for (var fixedManifest:all) {
                    var execution=(ExecutionFence)fixedManifest.executionIdentity();
                    plans.forRun(execution,proof).forget(scope.pipelineId());
                }
                Runnable retired=() -> {
                    proof.run();
                    if (captured.filter(Engine.Run::isCarryingAJob).isPresent()) throw fenced();
                    var observed=engine.captureRun(scope.pipelineId());
                    if (observed.isPresent()) {
                        var known=captured.or(() -> predecessor);
                        if (known.isEmpty() || observed.orElseThrow().jobId()!=known.orElseThrow().jobId()
                                || !observed.orElseThrow().operatorManifest().equals(known.orElseThrow().operatorManifest())
                                || observed.orElseThrow().isCarryingAJob()) throw fenced();
                    }
                    operators.requireRetired(scope,ceiling,proof); proof.run();
                };
                retired.run(); return new PipelineDeletionCoordinator.Isolated(isolated,retired,
                        () -> {active.run();control.requireHeld();active.run();});
            },(fixed,current,local) -> configuration.retireValidation(fixed,cache,current,local));
            try {
                if (pending.isPresent()) coordinator.resume(scope,request.operationId(),manifest).orElseThrow(NativePipelineDeletion::fenced);
                else coordinator.delete(request,manifest);
            } catch (TapstateException failed) {
                var current=configuration.artifacts().get(original.id());
                if (current.isPresent() && !CanonicalHash.of(current.orElseThrow()).equals(expectedHash)) throw conflict(original.id());
                throw failed;
            }
        }
    }

    private PipelineStateMapping mapping(String pipeline) { active.run(); return configuration.pipelineStates().read(pipeline).orElseThrow(NativePipelineDeletion::fenced); }
    private PipelineIntentScope scope(PipelineStateMapping mapping) {
        return new PipelineIntentScope(business.clusterId(),business.bindingVersion(),business.cacheIncarnation(),mapping.pipelineId(),mapping.stateGeneration());
    }
    private CheckpointDoc state(PipelineIntentScope scope) { return cache.pipelineStates(scope).read(scope.pipelineId()).orElseThrow(NativePipelineDeletion::fenced); }
    private void requireRest(PipelineIntentScope scope) {
        var preparation=configuration.preparations().read(scope.pipelineId()).orElseThrow(NativePipelineDeletion::fenced);
        var actual=preparation.phase()==PipelinePreparation.Phase.UNASSIGNED ? PipelineState.NEW : StateJson.parse(state(scope).stateJson());
        var intent=configuration.intents().load(scope.pipelineId()).filter(value -> !value.deleted());
        if (intent.isPresent() && !intent.orElseThrow().scope().equals(scope)) throw fenced();
        var desired=intent.map(value -> value.desired().targetState()).orElse(PipelineState.NEW);
        if (!LivePipelines.isAtRest(actual,desired)) throw new TapstateException(ArtifactError.PIPELINE_NOT_STOPPED,
                Map.of("id",scope.pipelineId(),"actual",actual.name(),"desired",desired.name()),null);
    }
    private void requireHash(String pipeline, String expected) {
        var current=configuration.artifacts().get(pipeline).orElseThrow(() -> new TapstateException(ArtifactError.NOT_FOUND,Map.of("id",pipeline),null));
        if (!CanonicalHash.of(current).equals(expected)) throw conflict(pipeline);
    }
    private static TapstateException conflict(String pipeline) { return new TapstateException(ArtifactError.VERSION_CONFLICT,Map.of("id",pipeline),null); }
    private static TapstateException fenced() { return ThreeLayerStorePort.refused(); }
}
