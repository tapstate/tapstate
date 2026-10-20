package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorNoteScopes;
import io.tapstate.adapters.pdk.ConnectorNoteOrigin;
import io.tapstate.core.model.SourceResource;
import java.util.function.BiFunction;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.PdkCapturePort;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.*;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Releases retained private connector resources before the original operation clears necessary notes. */
final class PipelinePrivateNoteRetirement {
    @FunctionalInterface interface Release {
        Optional<TapstateException> run(RequiredNoteRetirement original,CaptureConfig config,
                PipelineNode.Purpose purpose,String clusterId,Runnable requireOriginal);
    }
    private final RequiredNoteStore notes;
    private final PipelineStateRebuilds operations;
    private final Release release;
    private final BiFunction<String,Map<String,Object>,RequiredNoteOrigin> origins;

    PipelinePrivateNoteRetirement(RequiredNoteStore notes,PipelineStateRebuilds operations,ConnectorProvisioner connectors) {
        this(notes,operations,(id,settings) -> ConnectorNoteOrigin.capture(id,connectors.resolve(id).contentHash(),settings),pdkRelease(connectors));
    }
    private static Release pdkRelease(ConnectorProvisioner connectors) {
        Objects.requireNonNull(connectors);
        return (retirement,config,purpose,clusterId,guard) -> {
            var fixed=retirement.scope();var store=retirement.notes().guardedBy(guard);
            var global=store.open(new RequiredNoteOwner(fixed.owner().bindingVersion(),"pdk.global-state",
                    RequiredNoteOwner.Kind.GLOBAL,clusterId,0));
            var scopes=ConnectorNoteScopes.of(store,namespace -> {
                if(namespace.equals(fixed.owner().namespace()))return fixed;
                if(namespace.equals(global.owner().namespace()))return global;
                throw unavailable();
            },guard);
            return PdkCapturePort.withRequiredNotes(connectors,null,scopes).release(config,purpose);
        };
    }
    PipelinePrivateNoteRetirement(RequiredNoteStore notes,PipelineStateRebuilds operations,Release release) {
        this(notes,operations,(id,settings) -> {throw new IllegalStateException("no context resolver supplied");},release);
    }
    PipelinePrivateNoteRetirement(RequiredNoteStore notes,PipelineStateRebuilds operations,
            BiFunction<String,Map<String,Object>,RequiredNoteOrigin> origins,Release release) {
        this.origins=Objects.requireNonNull(origins);
        this.notes=Objects.requireNonNull(notes);this.operations=Objects.requireNonNull(operations);this.release=Objects.requireNonNull(release);
    }

    /** A cache-only rebuild preserves necessary identities; only a changed connection context is retired. */
    void reconcile(PipelineRebuildPreparation original,Map<String,SourceResource> desired,Runnable requireRetiredExecution) {
        requireRetiredExecution.run();operations.requirePending(original);var request=original.request();
        for(var context:notes.guardedBy(requireRetiredExecution).privateContexts(request.originalScope(),request.operationId())) {
            requireRetiredExecution.run();if(context.completed() || context.origin().isEmpty())continue;
            var source=desired.get(context.scope().owner().namespace());
            // A no-longer-drawn node still retains its necessary identity until an authorized deletion.
            if(source==null || context.origin().orElseThrow().equals(origins.apply(source.connector(),source.config())))continue;
            operations.requirePending(original);
            releaseOne(original.request().originalScope().clusterId(),notes.guardedBy(requireRetiredExecution).retirement(context.scope(),request.operationId()),requireRetiredExecution);
        }
        operations.requirePending(original);requireRetiredExecution.run();
    }

    /** The lifecycle caller has already terminated the original job, writers, capture and hot state. */
    void retire(PipelineRebuildPreparation original,Runnable requireRetiredExecution) {
        Objects.requireNonNull(original);Objects.requireNonNull(requireRetiredExecution);
        Runnable guard=requireRetiredExecution;guard.run();operations.requirePending(original);var request=original.request();
        for(var retirement:notes.guardedBy(guard).retirements(request.originalScope(),request.operationId())) {
            operations.requirePending(original);releaseOne(original.request().originalScope().clusterId(),retirement,guard);
        }
        operations.requirePending(original);guard.run();
    }
    /** Deletion has its own original lifecycle authority; a cache-only full never enters this path. */
    void delete(PipelineRetirementPreparation original,PipelineRetirements lifecycle,Runnable guard) {
        Objects.requireNonNull(original);Objects.requireNonNull(lifecycle);Objects.requireNonNull(guard);
        guard.run();lifecycle.requirePending(original);var request=original.request();
        for(var retirement:notes.guardedBy(guard).retirements(request.scope(),request.operationId())) {
            lifecycle.requirePending(original);releaseOne(request.scope().clusterId(),retirement,guard);
        }
        lifecycle.requirePending(original);guard.run();
    }

    private void releaseOne(String clusterId,RequiredNoteRetirement retirement,Runnable guard) {
        guard.run();if(retirement.completed())return;var origin=retirement.origin();
        if(origin.isPresent()) {
            var owner=retirement.scope().owner();
            var purpose=java.util.Arrays.stream(PipelineNode.Purpose.values()).filter(owner::hasPurpose).findFirst().orElseThrow(PipelinePrivateNoteRetirement::unavailable);
            String prefix="pdk.state."+owner.ownerId()+"."+purpose.segment()+".";
            var config=new CaptureConfig(origin.orElseThrow().connectorId(),Map.of(),List.of(),new PipelineNode(owner.ownerId(),owner.namespace().substring(prefix.length())));
            release.run(retirement,config,purpose,clusterId,guard).ifPresent(failure -> {throw failure;});guard.run();
        }
        retirement.completeAfterVerifiedRelease();guard.run();
    }
    private static TapstateException unavailable(){return new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null);}
}
