package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorNoteScopes;
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

    PipelinePrivateNoteRetirement(RequiredNoteStore notes,PipelineStateRebuilds operations,ConnectorProvisioner connectors) {
        this(notes,operations,(retirement,config,purpose,clusterId,guard) -> {
            var fixed=retirement.scope();var store=retirement.notes().guardedBy(guard);
            var global=store.open(new RequiredNoteOwner(fixed.owner().bindingVersion(),"pdk.global-state",
                    RequiredNoteOwner.Kind.GLOBAL,clusterId,0));
            var scopes=ConnectorNoteScopes.of(store,namespace -> {
                if(namespace.equals(fixed.owner().namespace()))return fixed;
                if(namespace.equals(global.owner().namespace()))return global;
                throw unavailable();
            },guard);
            return PdkCapturePort.withRequiredNotes(connectors,null,scopes).release(config,purpose);
        });
    }
    PipelinePrivateNoteRetirement(RequiredNoteStore notes,PipelineStateRebuilds operations,Release release) {
        this.notes=Objects.requireNonNull(notes);this.operations=Objects.requireNonNull(operations);this.release=Objects.requireNonNull(release);
    }

    /** The lifecycle caller has already terminated the original job, writers, capture and hot state. */
    void retire(PipelineRebuildPreparation original,Runnable requireRetiredExecution) {
        Objects.requireNonNull(original);Objects.requireNonNull(requireRetiredExecution);
        Runnable guard=() -> {requireRetiredExecution.run();operations.requirePending(original);requireRetiredExecution.run();};
        guard.run();var request=original.request();
        for(var retirement:notes.guardedBy(guard).retirements(request.originalScope(),request.operationId())) {
            guard.run();if(retirement.completed())continue;
            var origin=retirement.origin();
            if(origin.isPresent()) {
                var owner=retirement.scope().owner();
                var purpose=java.util.Arrays.stream(PipelineNode.Purpose.values()).filter(owner::hasPurpose).findFirst().orElseThrow(PipelinePrivateNoteRetirement::unavailable);
                String prefix="pdk.state."+owner.ownerId()+"."+purpose.segment()+".";
                var config=new CaptureConfig(origin.orElseThrow().connectorId(),Map.of(),List.of(),new PipelineNode(owner.ownerId(),owner.namespace().substring(prefix.length())));
                release.run(retirement,config,purpose,request.originalScope().clusterId(),guard).ifPresent(failure -> {throw failure;});guard.run();
            }
            retirement.completeAfterVerifiedRelease();guard.run();
        }
    }
    private static TapstateException unavailable(){return new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null);}
}
